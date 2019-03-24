package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormat;
import cal.bkup.types.Id;
import cal.bkup.types.RegularFile;
import cal.bkup.types.Sha256AndSize;
import cal.prim.ConsistentBlob;
import cal.prim.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.EventuallyConsistentBlobStore;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.InMemoryDir;
import cal.prim.InMemoryStringRegister;
import cal.prim.NoValue;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.DecryptedInputStream;
import cal.prim.transforms.StatisticsCollectingInputStream;
import cal.prim.transforms.XZCompression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;

@Test
public class BackupTests {

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
      public long sizeEstimateInBytes() {
        return 1024;
      }

      @Override
      public Object inode() {
        return this;
      }
    };

    backup.backup(system, password, password, Stream.of(f), Stream.empty(), Stream.empty());

    Assert.assertEquals(blobDir.list().count(), 1L);

    // blob compression should work
    try (InputStream s = blobDir.open(blobDir.list().findAny().get())) {
      long size = Util.drain(s);
      Assert.assertTrue(size < f.sizeEstimateInBytes(), "size(" + size + ") is not less than estimate(" + f.sizeEstimateInBytes() + ')');
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
    other.backup(system, password, newPassword, Stream.of(f), Stream.empty(), Stream.empty());

    // index is encrypted with new password
    try (DecryptedInputStream s = new DecryptedInputStream(Util.buffered(indexStore.read(indexStore.head())), newPassword)) {
      Util.drain(s);
    }

    // no new blob added
    Assert.assertEquals(blobDir.list().count(), 1L);

    // backup without `f`
    other.backup(system, newPassword, newPassword, Stream.empty(), Stream.empty(), Stream.empty());

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

  private static class BlobStoreOnDirectory implements EventuallyConsistentBlobStore {

    private final EventuallyConsistentDirectory data;

    public BlobStoreOnDirectory(EventuallyConsistentDirectory data) {
      this.data = data;
    }

    @Override
    public Stream<String> list() throws IOException {
      return data.list();
    }

    @Override
    public PutResult createOrReplace(InputStream stream) throws IOException {
      String id = UUID.randomUUID().toString();
      StatisticsCollectingInputStream in = new StatisticsCollectingInputStream(stream, (x) -> { });
      data.createOrReplace(id, in);
      return new PutResult() {
        @Override
        public String identifier() {
          return id;
        }

        @Override
        public long bytesStored() {
          return in.getBytesRead();
        }
      };
    }

    @Override
    public InputStream open(String name) throws IOException {
      return data.open(name);
    }

    @Override
    public void delete(String name) throws IOException {
      data.delete(name);
    }

  }

}
