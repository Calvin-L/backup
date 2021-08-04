package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormatV03;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.NoValue;
import cal.prim.Price;
import cal.prim.concurrency.InMemoryStringRegister;
import cal.prim.concurrency.StringRegister;
import cal.prim.fs.HardLink;
import cal.prim.fs.RegularFile;
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
import lombok.NonNull;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
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

  private static final IndexFormat FORMAT = new JsonIndexFormatV03();

  private void ensureWf(ConsistentBlob indexStore, ConsistentInMemoryDir blobDir, BlobTransformer transform, String password) throws IOException {
    BackupIndex index = new BackerUpper(indexStore, FORMAT, new BlobStoreOnDirectory(blobDir), transform, new TestClock()).getIndex(password);
    for (var sys : index.knownSystems()) {
      for (var p : index.knownPaths(sys)) {
        for (var rev : index.getInfo(sys, p)) {
          switch (rev.type) {
            case REGULAR_FILE:
              Assert.assertNotNull(blobDir.open(index.lookupBlob(rev.summary).getIdAtTarget()));
              Assert.assertNotNull(index.lookupBlob(rev.summary));
              break;
            case HARD_LINK:
              Assert.assertNotNull(index.resolveHardLinkTarget(sys, rev));
              break;
            case SOFT_LINK:
            case TOMBSTONE:
              break;
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

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null) {
      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }
    };

    Collection<Path> toForget = Collections.emptyList();
    backup.backup(system, password, password, Collections.singleton(f), Collections.emptyList(), Collections.emptyList(), toForget);

    Assert.assertEquals(blobDir.getPendingWrites().size(), 1);

    // blob compression should work
    try (InputStream s = readAny(blobDir)) {
      long size = Util.drain(s);
      Assert.assertTrue(size < f.getSizeInBytes(), "allegedly-compressed size(" + size + ") is not less than original(" + f.getSizeInBytes() + ')');
    }

    // blob encryption should work
    byte[] bytes;
    try (InputStream s = readAny(blobDir)) {
      bytes = Util.read(s);
    }
    Assert.assertNotEquals(bytes, Util.read(f.open()));

    // TODO: blob restore should work

    // restore of index should work
    BackerUpper other = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);

    Sha256AndSize summary;
    try (InputStream in = f.open()) {
      summary = Util.summarize(in, s -> { });
    }

    Assert.assertEquals(other.list(password).count(), 1L);
    other.list(password).forEach(info -> {
      Assert.assertEquals(info.path(), f.getPath());
      Assert.assertEquals(info.latestRevision().type, BackupIndex.FileType.REGULAR_FILE);
      Assert.assertEquals(info.latestRevision().summary, summary);
    });

    // backup with new password
    String newPassword = "fubar";
    Assert.assertNotEquals(password, newPassword);
    other.backup(system, password, newPassword, Collections.singleton(f), Collections.emptyList(), Collections.emptyList(), toForget);

    // index is encrypted with new password
    try (DecryptedInputStream s = new DecryptedInputStream(Util.buffered(indexStore.read(indexStore.head())), newPassword)) {
      Util.drain(s);
    }

    // no new blob added
    Assert.assertEquals(blobDir.getPendingWrites().size(), 1);

    // backup without `f`
    other.backup(system, newPassword, newPassword, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.singleton(f.getPath()));

    // assert empty
    other.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision().type);
    });
    Assert.assertEquals(other.list(newPassword).filter(info -> info.latestRevision().type != BackupIndex.FileType.TOMBSTONE).count(), 0L);

    // assert loadable
    backup = new BackerUpper(
            indexStore,
            FORMAT,
            new BlobStoreOnDirectory(blobDir),
            transform,
            UnreliableWallClock.SYSTEM_CLOCK);
    backup.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision().type);
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

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, data.length, null) {
      @Override
      public InputStream open() {
        return new ByteArrayInputStream(data);
      }
    };

    RegularFile g = new RegularFile(Paths.get("/", "tmp", "other"), Instant.now(), data.length, null) {
      @Override
      public InputStream open() {
        return new ByteArrayInputStream(data);
      }
    };

    Collection<Path> toForget = Collections.emptyList();
    backup.backup(system, password, password, Arrays.asList(f, g), Collections.emptyList(), Collections.emptyList(), toForget);

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

    Object inode1 = new Object();
    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, inode1) {
      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }
    };

    backup.planBackup(system, password, password, FREE,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList()).execute();

    Assert.assertEquals(blobDir.list().count(), 1L);

    Object inode2 = new Object();
    f = new RegularFile(Paths.get("/", "foo", "bar"), f.getModTime(), f.getSizeInBytes(), inode2) {
      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }
    };

    backup.planBackup(system, password, password, FREE,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList()).execute();

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

    RegularFile f = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null) {
      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }
    };

    RegularFile g = new RegularFile(Paths.get("/", "tmp", "file"), Instant.EPOCH, 1024, null) {
      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 42;
        }
        return new ByteArrayInputStream(data);
      }
    };

    // Start A, and wait a while...
    Thread a = Util.async(() -> {
      try {
        backupA.planBackup(systemA, password, password, FREE,
                Collections.singletonList(f),
                Collections.emptyList(),
                Collections.emptyList()).execute();
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
            Collections.emptyList()).execute();

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

  private static class TestFile extends RegularFile {
    private final byte[] contents;

    public TestFile(@NonNull Path path, @NonNull Instant modTime, Object iNode, byte[] contents) {
      super(path, modTime, contents.length, iNode);
      this.contents = contents;
    }

    @Override
    public InputStream open() {
      return new ByteArrayInputStream(contents);
    }
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
    var f = new TestFile(Paths.get("tmp", "a"), fCreation, 1, "contents".getBytes(StandardCharsets.UTF_8));

    backup.backup(system, password, password,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    var index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 1);
    Assert.assertEquals(index.get(0).latestRevision().type, BackupIndex.FileType.REGULAR_FILE);

    // 10 days later, clean up everything older than 5 days
    clock.timePasses(Duration.ofDays(10));
    backup.planCleanup(password, Duration.ofDays(5), FREE).execute();

    // file should still be there
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 1);
    Assert.assertEquals(index.get(0).latestRevision().type, BackupIndex.FileType.REGULAR_FILE);
    Assert.assertEquals(blobDir.list().count(), 1);

    backup.backup(system, password, password,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(f.getPath()));

    // file should be tombstoned
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 1);
    Assert.assertEquals(index.get(0).latestRevision().type, BackupIndex.FileType.TOMBSTONE);
    Assert.assertEquals(blobDir.list().count(), 1);

    // 5 days later, clean up everything older than 15 days
    clock.timePasses(Duration.ofDays(5));
    backup.planCleanup(password, Duration.ofDays(15), FREE).execute();

    // file should still be there
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 1);
    Assert.assertEquals(index.get(0).latestRevision().type, BackupIndex.FileType.TOMBSTONE);
    Assert.assertEquals(blobDir.list().count(), 1);

    // clean up everything older than 10 days
    backup.planCleanup(password, Duration.ofDays(10), FREE).execute();

    // file should be gone
    index = backup.list(password).collect(Collectors.toList());
    Assert.assertEquals(index.size(), 0);
    ensureWf(indexStore, blobDir, transform, password);

    System.out.println("index dir = " + indexDir);
    System.out.println("blob dir = " + blobDir);

    // cleanup should retain the newest index, and may retain others
    Assert.assertTrue(indexDir.list().count() >= 1);

    // cleanup should drop unused blobs
    Assert.assertEquals(blobDir.list().count(), 0);
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
    var file = new TestFile(Paths.get("tmp", "a"), fCreation, 1, "contents".getBytes(StandardCharsets.UTF_8));
    var link = new HardLink(Paths.get("home", "user", "file"), file.getPath());

    backup.backup(system, password, password,
            Collections.singletonList(file),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    clock.timePasses(Duration.ofDays(10));

    backup.backup(system, password, password,
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
    var fileA = new TestFile(Paths.get("tmp", "a"), clock.now(), 1, contents);
    var fileB = new TestFile(Paths.get("tmp", "b"), clock.now(), 2, contents);

    backup.backup(system, password, password,
            Collections.singletonList(fileA),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    backup.backup(system, password, password,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(fileA.getPath()));

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
    var fileA = new TestFile(Paths.get("tmp", "a"), clock.now(), 1, contents);
    var fileB = new TestFile(Paths.get("tmp", "b"), clock.now(), 2, contents);

    backup.backup(system, password, password,
            Collections.singletonList(fileA),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());

    backup.backup(system, password, password,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singletonList(fileA.getPath()));

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
    var fileA = new TestFile(Paths.get("tmp", "a"), fCreation, 1, "contents".getBytes(StandardCharsets.UTF_8));
    var fileB = new TestFile(Paths.get("tmp", "b"), fCreation, 2, "contents".getBytes(StandardCharsets.UTF_8));

    backup.backup(system, password, password,
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
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.singleton(fileB.getPath()));

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

}
