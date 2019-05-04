package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormat;
import cal.bkup.types.Id;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.prim.BlobStoreOnDirectory;
import cal.prim.ConsistentBlob;
import cal.prim.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.InMemoryDir;
import cal.prim.InMemoryStringRegister;
import cal.prim.NoValue;
import cal.prim.Price;
import cal.prim.StringRegister;
import cal.prim.fs.RegularFile;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.DecryptedInputStream;
import cal.prim.transforms.XZCompression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

@Test
public class BackupTests {

  private static final StorageCostModel FREE = new StorageCostModel() {
    @Override
    public Price costToUploadBlob(long numBytes) {
      return Price.ZERO;
    }

    @Override
    public Price monthlyStorageCostForBlob(long numBytes) {
      return Price.ZERO;
    }
  };

  @Test
  public void test() throws IOException, NoValue {

    final Id system = new Id("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final EventuallyConsistentDirectory blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);

    RegularFile f = new RegularFile() {
      @Override
      public Path path() {
        return Paths.get("/", "tmp", "file");
      }

      @Override
      public Instant modTime() {
        return Instant.EPOCH;
      }

      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }

      @Override
      public long sizeInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
      }
    };

    Collection<Path> toForget = Collections.emptyList();
    backup.backup(system, password, password, Collections.singleton(f), Collections.emptyList(), Collections.emptyList(), toForget);

    Assert.assertEquals(blobDir.list().count(), 1L);

    // blob compression should work
    try (InputStream s = blobDir.open(blobDir.list().findAny().get())) {
      long size = Util.drain(s);
      Assert.assertTrue(size < f.sizeInBytes(), "allegedly-compressed size(" + size + ") is not less than original(" + f.sizeInBytes() + ')');
    }

    // blob encryption should work
    byte[] bytes;
    try (InputStream s = blobDir.open(blobDir.list().findAny().get())) {
      bytes = Util.read(s);
    }
    Assert.assertNotEquals(bytes, Util.read(f.open()));

    // TODO: blob restore should work

    // restore of index should work
    BackerUpper other = new BackerUpper(
            indexStore,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);

    Sha256AndSize summary;
    try (InputStream in = f.open()) {
      summary = Util.summarize(in, s -> { });
    }

    Assert.assertEquals(other.list(password).count(), 1L);
    other.list(password).forEach(info -> {
      Assert.assertEquals(info.path(), f.path());
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
    Assert.assertEquals(blobDir.list().count(), 1L);

    // backup without `f`
    other.backup(system, newPassword, newPassword, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.singleton(f.path()));

    // assert empty
    other.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision().type);
    });
    Assert.assertEquals(other.list(newPassword).filter(info -> info.latestRevision().type != BackupIndex.FileType.TOMBSTONE).count(), 0L);

    // assert loadable
    backup = new BackerUpper(
            indexStore,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);
    backup.list(newPassword).forEach(info -> {
      System.out.println("latest revision for " + info.path() + ": " + info.latestRevision().type);
    });

  }

  @Test
  public void testMove() throws IOException {

    final Id system = new Id("foobar");
    final String password = "fizzbuzz";

    final ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(new InMemoryStringRegister(), new InMemoryDir());
    final EventuallyConsistentDirectory blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backup = new BackerUpper(
            indexStore,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);

    RegularFile f = new RegularFile() {
      @Override
      public Path path() {
        return Paths.get("/", "tmp", "file");
      }

      @Override
      public Instant modTime() {
        return Instant.EPOCH;
      }

      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }

      @Override
      public long sizeInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
      }
    };

    backup.planBackup(system, password, password, FREE,
            Collections.singletonList(f),
            Collections.emptyList(),
            Collections.emptyList()).execute();

    Assert.assertEquals(blobDir.list().count(), 1L);

    f = new RegularFile() {
      @Override
      public Path path() {
        return Paths.get("/", "foo", "bar");
      }

      @Override
      public Instant modTime() {
        return Instant.EPOCH;
      }

      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }

      @Override
      public long sizeInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
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

  @Test(enabled = false)
  public void testConcurrentBackup() throws IOException, InterruptedException {

    final Id systemA = new Id("foobar");
    final Id systemB = new Id("barfoo");
    final String password = "fizzbuzz";

    final StringRegister register = new InMemoryStringRegister();
    final EventuallyConsistentDirectory indexDirB = new InMemoryDir();
    final LatchedDir indexDirA = new LatchedDir(indexDirB);
    final ConsistentBlob indexStoreA = new ConsistentBlobOnEventuallyConsistentDirectory(register, indexDirA);
    final ConsistentBlob indexStoreB = new ConsistentBlobOnEventuallyConsistentDirectory(register, indexDirB);
    final EventuallyConsistentDirectory blobDir = new InMemoryDir();

    BlobTransformer transform = new XZCompression();
    BackerUpper backupA = new BackerUpper(
            indexStoreA,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);
    BackerUpper backupB = new BackerUpper(
            indexStoreB,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);

    RegularFile f = new RegularFile() {
      @Override
      public Path path() {
        return Paths.get("/", "tmp", "file");
      }

      @Override
      public Instant modTime() {
        return Instant.EPOCH;
      }

      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 33;
        }
        return new ByteArrayInputStream(data);
      }

      @Override
      public long sizeInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
      }
    };

    RegularFile g = new RegularFile() {
      @Override
      public Path path() {
        return Paths.get("/", "tmp", "file");
      }

      @Override
      public Instant modTime() {
        return Instant.EPOCH;
      }

      @Override
      public InputStream open() {
        byte[] data = new byte[1024];
        for (int i = 0; i < data.length; ++i) {
          data[i] = 42;
        }
        return new ByteArrayInputStream(data);
      }

      @Override
      public long sizeInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
      }
    };

    // Start A, and wait a while...
    Thread a = Util.async(() -> {
      try {
        backupA.planBackup(systemA, password, password, FREE,
                Collections.singletonList(f),
                Collections.emptyList(),
                Collections.emptyList()).execute();
      } catch (IOException e) {
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
    indexDirA.letOneThrough();
    a.join();

    // Both blobs should be backed up
    Assert.assertEquals(blobDir.list().count(), 2L);

    BackerUpper backupC = new BackerUpper(
            indexStoreB,
            new JsonIndexFormat(),
            new BlobStoreOnDirectory(blobDir),
            transform);

    System.out.println("Backed up stuff:");
    backupC.list(password).forEach(thing -> System.out.println(" - " + thing.path() + " on " + thing.system()));

    Assert.assertEquals(backupC.list(password).count(), 2L);

  }

}
