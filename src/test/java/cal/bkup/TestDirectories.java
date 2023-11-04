package cal.bkup;

import cal.prim.storage.EventuallyConsistentDirectory;
import cal.prim.storage.InMemoryDir;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.XZCompression;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

@Test
public class TestDirectories {

  private final Charset CHARSET = StandardCharsets.UTF_8;

  class TransformedDirectory implements EventuallyConsistentDirectory {

    private final EventuallyConsistentDirectory directory;
    private final BlobTransformer transform;

    /**
     * Apply the given transformations to data placed in the directory.
     *
     * @param directory the directory to wrap
     * @param transform the transformation to apply
     * @return
     */
    public TransformedDirectory(EventuallyConsistentDirectory directory, BlobTransformer transform) {
      this.directory = directory;
      this.transform = transform;
    }

    @Override
    public Stream<String> list() throws IOException {
      return directory.list();
    }

    @Override
    public void createOrReplace(String name, InputStream stream) throws IOException {
      directory.createOrReplace(name, transform.apply(stream));
    }

    @Override
    @SuppressWarnings("required.method.not.called") // TODO: false positive?
    public InputStream open(String name) throws IOException {
      return transform.unApply(directory.open(name));
    }

    @Override
    public void delete(String name) throws IOException {
      directory.delete(name);
    }

  }

  private void check(EventuallyConsistentDirectory dir) throws IOException {
    String text = "as;gihawpognaw;efe;";
    String key = "foo";
    System.out.println("starting");
    try (InputStream s = new ByteArrayInputStream(text.getBytes(CHARSET))) {
      dir.createOrReplace(key, s);
    }
    System.out.println("done");
    while (true) {
      try (InputStream file = dir.open(key);
           InputStreamReader rawReader = new InputStreamReader(file, CHARSET);
           BufferedReader r = new BufferedReader(rawReader)) {
        String line = r.readLine();
        if (line != null) {
          Assert.assertEquals(line, text);
        }
        return;
      } catch (NoSuchFileException ignored) {
      }
    }
  }

  @Test
  public void testBasic() throws Exception {
    check(new InMemoryDir());
  }

  @Test
  public void testEncrypt() throws Exception {
    check(new TransformedDirectory(new InMemoryDir(), new cal.prim.transforms.Encryption(Encryption.password)));
  }

  @Test
  public void testXZ() throws Exception {
    check(new TransformedDirectory(new InMemoryDir(), new XZCompression()));
  }

  @Test
  public void testXZEncrypt() throws Exception {
    check(new TransformedDirectory(new InMemoryDir(), new XZCompression().followedBy(new cal.prim.transforms.Encryption(Encryption.password))));
  }

  @Test
  public void testTransformOrder() throws Exception {
    EventuallyConsistentDirectory coreDir = new InMemoryDir();
    BlobTransformer compression = new XZCompression();
    BlobTransformer encryption = new cal.prim.transforms.Encryption(Encryption.password);
    EventuallyConsistentDirectory view = new TransformedDirectory(coreDir, compression.followedBy(encryption));
    byte[] originalBytes = "hello, world".getBytes(CHARSET);
    view.createOrReplace("foo", new ByteArrayInputStream(originalBytes));
    byte[] rawBytes;
    while (true) {
      try (InputStream in = coreDir.open("foo")) {
        rawBytes = Util.read(in);
        break;
      } catch (NoSuchFileException ignored) {
      }
    }

    // The raw bytes can be decrypted...
    byte[] decryptedBytes;
    try (InputStream in = encryption.unApply(new ByteArrayInputStream(rawBytes))) {
      decryptedBytes = Util.read(in);
    }

    // ...and then decompressed...
    byte[] decompressedBytes;
    try (InputStream in = compression.unApply(new ByteArrayInputStream(decryptedBytes))) {
      decompressedBytes = Util.read(in);
    }

    // ...to obtain the original data.
    Assert.assertEquals(decompressedBytes, originalBytes);
  }

}
