package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormat;
import cal.bkup.types.Id;
import cal.bkup.types.StorageCostModel;
import cal.prim.Price;
import cal.prim.fs.RegularFile;
import cal.bkup.types.Sha256AndSize;
import cal.prim.BlobStoreOnDirectory;
import cal.prim.ConsistentBlob;
import cal.prim.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.InMemoryDir;
import cal.prim.InMemoryStringRegister;
import cal.prim.NoValue;
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

}
