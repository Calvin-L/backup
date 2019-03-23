package cal.bkup;

import cal.prim.EventuallyConsistentDirectory;
import cal.prim.InMemoryDir;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.TransformedDirectory;
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

@Test
public class TestDirectories {

  private final Charset CHARSET = StandardCharsets.UTF_8;

  private void check(EventuallyConsistentDirectory dir) throws IOException {
    String text = "as;gihawpognaw;efe;";
    String key = "foo";
    System.out.println("starting");
    try (InputStream s = new ByteArrayInputStream(text.getBytes(CHARSET))) {
      dir.createOrReplace(key, s);
    }
    System.out.println("done");
    try (BufferedReader r = new BufferedReader(new InputStreamReader(dir.open(key), CHARSET))) {
      Assert.assertEquals(r.readLine(), text);
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
    byte[] rawBytes = Util.read(coreDir.open("foo"));

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
