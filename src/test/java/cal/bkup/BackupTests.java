package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormatV04;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.IOConsumer;
import cal.prim.NoValue;
import cal.prim.Price;
import cal.prim.concurrency.InMemoryStringRegister;
import cal.prim.concurrency.StringRegister;
import cal.prim.fs.Filesystem;
import cal.prim.fs.HardLink;
import cal.prim.fs.RegularFile;
import cal.prim.fs.SymLink;
import cal.prim.storage.BlobStoreOnDirectory;
import cal.prim.storage.ConsistentBlob;
import cal.prim.storage.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.storage.ConsistentInMemoryDir;
import cal.prim.storage.EventuallyConsistentDirectory;
import cal.prim.storage.InMemoryDir;
import cal.prim.time.UnreliableWallClock;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.DecryptedInputStream;
import cal.prim.transforms.XZCompression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Test
public class BackupTests {

  private static final StorageCostModel FREE = new StorageCostModel() {
    @Override
    public Price costToUploadBlob(long numBytes) {
      return Price.ZERO;
    }

    @Override
    public Price costToDeleteBlob(long numBytes, Duration timeSinceUpload) {
      return Price.ZERO;
    }

    @Override
    public Price monthlyStorageCostForBlob(long numBytes) {
      return Price.ZERO;
    }
  };

  private static final IndexFormat FORMAT = new JsonIndexFormatV04();

  private void ensureWf(ConsistentBlob indexStore, ConsistentInMemoryDir blobDir, BlobTransformer transform, String password) throws IOException {
    BackupIndex index = new BackerUpper(indexStore, FORMAT, new BlobStoreOnDirectory(blobDir), transform, new TestClock()).getIndex(password);
    for (var sys : index.knownSystems()) {
      for (var p : index.knownPaths(sys)) {
        for (var rev : index.getInfo(sys, p)) {
          if (rev instanceof BackupIndex.RegularFileRev f) {
            var blobInfo = index.lookupBlob(f.summary());
            if (blobInfo == null) {
              throw new RuntimeException("Missing blob info for " + f);
            }
            Assert.assertNotNull(blobDir.open(blobInfo.idAtTarget()));
            Assert.assertNotNull(index.lookupBlob(f.summary()));
          } else if (rev instanceof BackupIndex.HardLinkRev l) {
            Assert.assertNotNull(index.resolveHardLinkTarget(sys, l));
          }
        }
      }
    }
  }

  private InputStream readAny(EventuallyConsistentDirectory store) throws IOException {
    for (;;) {
      try {
        return store.open(store.list().findAny().get());
      } catch (NoSuchFileException | NoSuchElementException ignored) {
        // NoSuchElementException --> the list() call did not see the object
        // NoSuchFileException --> the open() call did not see the object
      }
    }
  }

  private static abstract class FakeFilesystem implements Filesystem {
    @Override
    public void scan(Path path, Set<PathMatcher> exclusions, IOConsumer<SymLink> onSymlink, IOConsumer<RegularFile> onFile) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  public void test() throws IOException, NoValue, BackupIndex.MergeConflict, ConsistentBlob.TagExpired {
    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final InMemoryDir blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 33);
        return new ByteArrayInputStream(data);
      }
    };

    Collection<Path> toForget = Collections.emptyList();
    backup.backup(system, password, password, fs, Collections.singleton(f), Collections.emptyList(), Collections.emptyList(), toForget);

    Assert.assertEquals(blobDir.getPendingWrites().size(), 1);

    // blob compression should work
    try (InputStream s = readAny(blobDir)) {
      long size = Util.drain(s);
      Assert.assertTrue(size < f.sizeInBytes(), "allegedly-compressed size(" + size + ") is not less than original(" + f.sizeInBytes() + ')');
    }

    // blob encryption should work
    byte[] bytes;
    try (InputStream s = readAny(blobDir)) {
      bytes = Util.read(s);
    }
    Assert.assertNotEquals(bytes, Util.read(fs.openRegularFileForReading(f.path())));

    // TODO: blob restore should work

    // restore of index should work
    BackerUpper other = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    Sha256AndSize summary;
    try (InputStream in = fs.openRegularFileForReading(f.path())) {
      summary = Util.summarize(in, s -> { });
    }

    Assert.assertEquals(other.list(password).count(), 1L);
    other.list(password).forEach(info -> {
      Assert.assertEquals(info.path(), f.path());
      Assert.assertTrue(info.latestRevision() instanceof BackupIndex.RegularFileRev);
      Assert.assertEquals(((BackupIndex.RegularFileRev) info.latestRevision()).summary(), summary);
    });

    // backup with new password
    String newPassword = "fubar";
    Assert.assertNotEquals(password, newPassword);
    other.backup(system, password, newPassword, fs, Collections.singleton(f), Collections.emptyList(), Collections.emptyList(), toForget);

    // index is encrypted with new password
    try (DecryptedInputStream s = new DecryptedInputStream(Util.buffered(indexStore.read(indexStore.head())), newPassword)) {
      Util.drain(s);
    }

    // no new blob added
    Assert.assertEquals(blobDir.getPendingWrites().size(), 1);

    // backup without `f`
    other.backup(system, newPassword, newPassword, fs, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.singleton(f.path()));

    // assert empty
    other.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision());
    });
    Assert.assertEquals(
        other.list(newPassword).filter(info -> !(info.latestRevision() instanceof BackupIndex.TombstoneRev)).count(),
        0L);

    // assert loadable
    backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);
    backup.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision());
    });

  }

  @Test
  public void testDuplicateFiles() throws IOException, BackupIndex.MergeConflict {

    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final InMemoryDir blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    byte[] data = new byte[1024];
    Arrays.fill(data, (byte) 33);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        return new ByteArrayInputStream(data);
      }
    };

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, data.length, null);

    RegularFile g = new RegularFile(Paths.get("/", "tmp", "other"), Instant.now(), data.length, null);

    Collection<Path> toForget = Collections.emptyList();
    backup.backup(system, password, password, fs, Arrays.asList(f, g), Collections.emptyList(), Collections.emptyList(), toForget);

    // Only one blob should be uploaded
    Assert.assertEquals(blobDir.getPendingWrites().size(), 1);
  }

  @Test
  public void testMove() throws IOException, BackupIndex.MergeConflict {

    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final EventuallyConsistentDirectory blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 33);
        return new ByteArrayInputStream(data);
      }
    };

    Object inode1 = new Object();
    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, inode1);

    backup.planBackup(system, password, password, FREE,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList()).execute(fs);

    Assert.assertEquals(blobDir.list().count(), 1L);

    Object inode2 = new Object();
    f = new RegularFile(Paths.get("/", "foo", "bar"), f.modTime(), f.sizeInBytes(), inode2);

    backup.planBackup(system, password, password, FREE,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList()).execute(fs);

    Assert.assertEquals(blobDir.list().count(), 1L);

  }

  private static class LatchedDir implements EventuallyConsistentDirectory {

    private final EventuallyConsistentDirectory wrapped;
    private final AtomicBoolean open;
    private final Object monitor;

    public LatchedDir(EventuallyConsistentDirectory wrapped) {
      this.wrapped = wrapped;
      this.open = new AtomicBoolean(true);
      this.monitor = new Object();
    }

    public void letOneThrough() {
      open.getAndSet(true);
      synchronized (monitor) {
        monitor.notify();
      }
    }

    @Override
    public Stream<String> list() throws IOException {
      return wrapped.list();
    }

    @Override
    public InputStream open(String name) throws IOException {
      return wrapped.open(name);
    }

    @Override
    public void delete(String name) throws IOException {
      wrapped.delete(name);
    }

    @Override
    public void createOrReplace(String name, InputStream s) throws IOException {
      while (!open.compareAndSet(true, false)) {
        try {
          synchronized (monitor) {
            monitor.wait();
          }
        } catch (InterruptedException ignored) {
        }
      }
      wrapped.createOrReplace(name, s);
    }
  }

  @Test
  public void testConcurrentBackup() throws IOException, InterruptedException, BackupIndex.MergeConflict {

    final SystemId systemA = new SystemId("foobar");
    final SystemId systemB = new SystemId("barfoo");
    final String password = "fizzbuzz";

    final StringRegister register = new InMemoryStringRegister();
    final EventuallyConsistentDirectory indexDirB = new InMemoryDir();
    final LatchedDir indexDirA = new LatchedDir(indexDirB);
    final ConsistentBlob indexStoreA = new ConsistentBlobOnEventuallyConsistentDirectory(register, indexDirA);
    final ConsistentBlob indexStoreB = new ConsistentBlobOnEventuallyConsistentDirectory(register, indexDirB);
    final InMemoryDir blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backupA = new BackerUpper(
            indexStoreA,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);
    BackerUpper backupB = new BackerUpper(
            indexStoreB,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null);

    RegularFile g = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null);

    Filesystem fs1 = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 33);
        return new ByteArrayInputStream(data);
      }
    };

    Filesystem fs2 = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 42);
        return new ByteArrayInputStream(data);
      }
    };

    // Start A, and wait a while...
    Thread a = Util.async(() -> {
      try {
        backupA.planBackup(systemA, password, password, FREE,
                Collections.singletonList(f),
                Collections.emptyList(),
                Collections.emptyList()).execute(fs1);
      } catch (IOException | BackupIndex.MergeConflict e) {
        throw new RuntimeException(e);
      }
    });

    // eh, should be long enough...
    Thread.sleep(250);

    // Let B finish
    backupB.planBackup(systemB, password, password, FREE,
            Collections.singletonList(g),
            Collections.emptyList(),
            Collections.emptyList()).execute(fs2);

    // Let A finish writing
    while (a.isAlive()) {
      indexDirA.letOneThrough();
      Thread.sleep(5);
    }
    a.join();

    // Both blobs should be backed up
    Assert.assertEquals(blobDir.getPendingWrites().size(), 2);

    BackerUpper backupC = new BackerUpper(
            indexStoreB,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    System.out.println("Backed up stuff:");
    backupC.list(password).forEach(thing -> System.out.println(" - " + thing.path() + " on " + thing.system()));

    Assert.assertEquals(backupC.list(password).count(), 2L);
    ensureWf(indexStoreB, blobDir.settle(), transform, password);

  }

  private static class TestClock implements UnreliableWallClock {
    Instant now = Instant.EPOCH;

    @Override
    public synchronized Instant now() {
      return now;
    }

    public synchronized void timePasses(Duration time) {
      assert !time.isNegative();
      now = now.plus(time);
    }
  }

  @Test
  public void testCleanup() throws IOException, BackupIndex.MergeConflict {

    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    Instant fCreation = clock.now();
    var f = new RegularFile(Paths.get("tmp", "a"), fCreation, 1, 100);
    var g = new RegularFile(Paths.get("tmp", "b"), fCreation, 9, 101);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        if (path.equals(f.path())) {
          return new ByteArrayInputStream("contents".getBytes(StandardCharsets.UTF_8));
        } else if (path.equals(g.path())) {
          return new ByteArrayInputStream("foo".getBytes(StandardCharsets.UTF_8));
        } else {
          throw new IllegalArgumentException(path.toString());
        }
      }
    };

    backup.backup(system, password, password,
            fs,
            Arrays.asList(f, g),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    var index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertTrue(index.get(0).latestRevision() instanceof BackupIndex.RegularFileRev);

    // 10 days later, clean up everything older than 5 days
    clock.timePasses(Duration.ofDays(10));
    backup.planCleanup(password, Duration.ofDays(5), FREE).execute();

    // file should still be there
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertTrue(index.get(0).latestRevision() instanceof BackupIndex.RegularFileRev);
    Assert.assertTrue(blobDir.list().findAny().isPresent());

    backup.backup(system, password, password,
            fs,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Arrays.asList(f.path(), g.path()));

    // file should be tombstoned
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertTrue(index.get(0).latestRevision() instanceof BackupIndex.TombstoneRev);
    Assert.assertTrue(blobDir.list().findAny().isPresent());

    // 5 days later, clean up everything older than 15 days
    clock.timePasses(Duration.ofDays(5));
    backup.planCleanup(password, Duration.ofDays(15), FREE).execute();

    // file should still be there
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertTrue(index.get(0).latestRevision() instanceof BackupIndex.TombstoneRev);
    Assert.assertTrue(blobDir.list().findAny().isPresent());

    // clean up everything older than 10 days
    var plan = backup.planCleanup(password, Duration.ofDays(10), FREE);
    Assert.assertEquals(plan.totalBlobsReclaimed(), blobDir.list().count());
    Assert.assertEquals(plan.bytesReclaimed(), blobDir.list().mapToLong(s -> {
      long count = 0;
      try (InputStream in = blobDir.open(s)) {
        while (in.read() >= 0) {
          ++count;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return count;
    }).sum());
    Assert.assertEquals(plan.untrackedBlobsReclaimed(), 0);
    plan.execute();

    // file should be gone
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 0);
    ensureWf(indexStore, blobDir, transform, password);

    System.out.println("index dir = " + indexDir);
    System.out.println("blob dir = " + blobDir);

    // cleanup should retain the newest index, and may retain others
    Assert.assertTrue(indexDir.list().findAny().isPresent());

    // cleanup should drop unused blobs
    Assert.assertTrue(blobDir.list().findAny().isEmpty());
    ensureWf(indexStore, blobDir, transform, password);

  }

  @Test
  public void testCleanupRetainsHardLinkTargets() throws IOException, BackupIndex.MergeConflict {
    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    Instant fCreation = clock.now();
    var file = new RegularFile(Paths.get("tmp", "a"), fCreation, 1, 100);
    var link = new HardLink(Paths.get("home", "user", "file"), file.path());

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        return new ByteArrayInputStream("contents".getBytes(StandardCharsets.UTF_8));
      }
    };

    backup.backup(system, password, password,
            fs,
            Collections.singletonList(file),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    clock.timePasses(Duration.ofDays(10));

    backup.backup(system, password, password,
            fs,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(link),
            Collections.emptyList());

    backup.planCleanup(password, Duration.ofDays(5), FREE).execute();

    // The file should not be deleted!
    Assert.assertEquals(blobDir.list().count(), 1);
    ensureWf(indexStore, blobDir, transform, password);
  }

  @Test
  public void testCleanupFencesOutBackups() throws IOException, BackupIndex.MergeConflict {
    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    var contents = "contents".getBytes(StandardCharsets.UTF_8);
    var fileA = new RegularFile(Paths.get("tmp", "a"), clock.now(), 1, 100);
    var fileB = new RegularFile(Paths.get("tmp", "b"), clock.now(), 2, 101);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        return new ByteArrayInputStream(contents);
      }
    };


    backup.backup(system, password, password,
            fs,
            Collections.singletonList(fileA),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    backup.backup(system, password, password,
            fs,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(fileA.path()));

    clock.timePasses(Duration.ofDays(10));

    // file is now eligible for cleanup
    // in parallel: try do do a backup that resurrects its blob, and a cleanup that removes it (the cleanup wins)

    BackerUpper cleanup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);
    cleanup.planCleanup(password, Duration.ofDays(5), FREE).execute();
    Assert.assertEquals(blobDir.list().count(), 0);

    BigInteger gen = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock).getIndex(password).getCleanupGeneration();
    Assert.assertNotEquals(gen, backup.getIndex(password).getCleanupGeneration());

    boolean failedDueToMergeConflict = false;
    try {
      backup.backup(system, password, password,
              fs,
              Collections.singletonList(fileB),
              Collections.emptyList(),
              Collections.emptyList(),
              Collections.emptyList());
    } catch (BackupIndex.MergeConflict ignored) {
      failedDueToMergeConflict = true;
    }

    Assert.assertTrue(failedDueToMergeConflict);
    Assert.assertEquals(blobDir.list().count(), 0);

    // 2nd try should succeed, and re-upload the blob
    backup.backup(system, password, password,
            fs,
            Collections.singletonList(fileB),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    Assert.assertEquals(blobDir.list().count(), 1);
    ensureWf(indexStore, blobDir, transform, password);
  }

  @Test
  public void testCleanupFencesOutConcurrentCleanups() throws IOException, BackupIndex.MergeConflict {
    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    var contents = "contents".getBytes(StandardCharsets.UTF_8);
    var fileA = new RegularFile(Paths.get("tmp", "a"), clock.now(), 1, 100);
    var fileB = new RegularFile(Paths.get("tmp", "b"), clock.now(), 2, 101);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        return new ByteArrayInputStream(contents);
      }
    };


    backup.backup(system, password, password,
            fs,
            Collections.singletonList(fileA),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    backup.backup(system, password, password,
            fs,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(fileA.path()));

    clock.timePasses(Duration.ofDays(10));

    // file is now eligible for cleanup
    // in parallel: try do do
    //   - a backup that resurrects its blob followed by a cleanup, and
    //   - a cleanup that removes it
    // assuming the backup+cleanup "wins" the race.
    // The followup cleanup should not succeed.

    BackerUpper cleanup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);
    var cleanupThatShouldFail = cleanup.planCleanup(password, Duration.ofDays(5), FREE);

    backup.backup(system, password, password,
            fs,
            Collections.singletonList(fileB),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());
    backup.planCleanup(password, Duration.ofDays(5), FREE).execute();

    Assert.assertThrows(BackupIndex.MergeConflict.class, cleanupThatShouldFail::execute);
    Assert.assertEquals(blobDir.list().count(), 1);
    ensureWf(indexStore, blobDir, transform, password);
  }


  @Test
  public void testCleanupDoesNotDeleteBlobsWithMultipleRefs() throws IOException, BackupIndex.MergeConflict {

    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    Instant fCreation = clock.now();
    var fileA = new RegularFile(Paths.get("tmp", "a"), fCreation, 1, 100);
    var fileB = new RegularFile(Paths.get("tmp", "b"), fCreation, 2, 101);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {
        return new ByteArrayInputStream("contents".getBytes(StandardCharsets.UTF_8));
      }
    };

    backup.backup(system, password, password,
            fs,
            Arrays.asList(fileA, fileB),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    var index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertEquals(blobDir.list().count(), 1);

    // next day, tombstone B
    clock.timePasses(Duration.ofDays(1));
    backup.backup(system, password, password,
            fs,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(fileB.path()));

    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 2);
    Assert.assertEquals(blobDir.list().count(), 1);

    // 10 days later, clean up everything older than 5 days
    clock.timePasses(Duration.ofDays(10));
    backup.planCleanup(password, Duration.ofDays(5), FREE).execute();

    // contents of A should still be there
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 1);
    Assert.assertEquals(blobDir.list().count(), 1);
    ensureWf(indexStore, blobDir, transform, password);
  }

  @Test
  public void testCleanupRetainsOldTargetsOfHardLinks() throws IOException, BackupIndex.MergeConflict {

    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final var indexDir = new ConsistentInMemoryDir();
    final var blobDir = new ConsistentInMemoryDir();
    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), indexDir);
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    Instant fCreation = clock.now();
    var file1 = new RegularFile(Paths.get("tmp", "a"), fCreation, 1, 100);
    var file2 = new RegularFile(Paths.get("tmp", "c"), fCreation, 1, 100);
    var link = new HardLink(Paths.get("tmp", "c"), Paths.get("tmp", "a"));

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) {

        return new ByteArrayInputStream("contents".getBytes(StandardCharsets.UTF_8));
      }
    };

    backup.backup(system, password, password,
            fs,
            Collections.singletonList(file1),
            Collections.emptyList(),
            Collections.singletonList(link),
            Collections.emptyList());

    clock.timePasses(Duration.ofDays(5));
    backup.backup(system, password, password,
            fs,
            Collections.singletonList(file2),
            Collections.emptyList(),
            Collections.singletonList(link),
            Collections.singletonList(file1.path()));

    backup.planCleanup(password, Duration.ofDays(1), FREE).execute();
  }

  private static String stringify(InputStream data) throws IOException {
    try (InputStream is = data) {
      return new String(Util.read(is), StandardCharsets.UTF_8);
    }
  }

  @Test
  public void testRestore() throws IOException, BackupIndex.MergeConflict {
    final SystemId system = new SystemId("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final InMemoryDir blobDir = new InMemoryDir();
    final TestClock clock = new TestClock();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    Instant t1 = clock.now();
    clock.timePasses(Duration.ofHours(1));
    Instant t2 = clock.now();
    var fileA = new RegularFile(Paths.get("tmp", "a"), t1, 1, 100);
    var fileB = new RegularFile(Paths.get("tmp", "b"), t1, 2, 102);
    var fileC = new RegularFile(Paths.get("tmp", "c"), t2, 2, 103);

    Filesystem fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) throws NoSuchFileException {
        if (path.equals(fileA.path())) {
          return new ByteArrayInputStream("contents1".getBytes(StandardCharsets.UTF_8));
        } else if (path.equals(fileB.path())) {
          return new ByteArrayInputStream("contents1".getBytes(StandardCharsets.UTF_8));
        } else if (path.equals(fileC.path())) {
          return new ByteArrayInputStream("contents2".getBytes(StandardCharsets.UTF_8));
        } else {
          throw new NoSuchFileException(path.toString());
        }
      }
    };

    backup.backup(system, password, password,
            fs,
            Arrays.asList(fileA, fileB),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    fs = new FakeFilesystem() {
      @Override
      public InputStream openRegularFileForReading(Path path) throws NoSuchFileException {
        if (path.equals(fileA.path())) {
          return new ByteArrayInputStream("contents3".getBytes(StandardCharsets.UTF_8));
        } else if (path.equals(fileB.path())) {
          return new ByteArrayInputStream("contents1".getBytes(StandardCharsets.UTF_8));
        } else if (path.equals(fileC.path())) {
          return new ByteArrayInputStream("contents2".getBytes(StandardCharsets.UTF_8));
        } else {
          throw new NoSuchFileException(path.toString());
        }
      }
    };

    backup.backup(system, password, password,
            fs,
            Arrays.asList(fileC, fileA),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    BackerUpper backup2 = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            clock);

    BackupIndex index = backup2.getIndex(password);
    index.checkIntegrity();

    Assert.assertEquals(index.knownBackups(system).size(), 2);

    // need retries because blob dir is eventually-consistent

    for (;;) {
      try {
        Assert.assertEquals(
                stringify(backup2.restore(password, ((BackupIndex.RegularFileRev) index.getInfo(system, fileA.path()).get(0)).summary())),
                "contents1");
        break;
      } catch (NoSuchFileException ignored) {
        continue;
      }
    }

    for (;;) {
      try {
        Assert.assertEquals(
                stringify(backup2.restore(password, ((BackupIndex.RegularFileRev) index.getInfo(system, fileA.path()).get(1)).summary())),
                "contents3");
        break;
      } catch (NoSuchFileException ignored) {
        continue;
      }
    }

    for (;;) {
      try {
        Assert.assertEquals(
                stringify(backup2.restore(password, ((BackupIndex.RegularFileRev) index.getInfo(system, fileB.path()).get(0)).summary())),
                "contents1");
        break;
      } catch (NoSuchFileException ignored) {
        continue;
      }
    }

    for (;;) {
      try {
        Assert.assertEquals(
                stringify(backup2.restore(password, ((BackupIndex.RegularFileRev) index.getInfo(system, fileC.path()).get(0)).summary())),
                "contents2");
        break;
      } catch (NoSuchFileException ignored) {
        continue;
      }
    }
  }

}
