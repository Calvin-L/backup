package cal.bkup;

import cal.prim.EventuallyConsistentDirectory;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

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

  private static class InMemoryDir implements EventuallyConsistentDirectory {
    private final Map<String, byte[]> data = new HashMap<>();

    @Override
    public Stream<String> list() {
      return data.keySet().stream();
    }

    @Override
    public void createOrReplace(String name, InputStream s) throws IOException {
      byte[] bytes = Util.read(s);
      System.out.println("dir[" + name + "] = " + Arrays.toString(bytes));
      data.put(name, bytes);
    }

    @Override
    public InputStream open(String name) {
      return new ByteArrayInputStream(data.get(name));
    }

    @Override
    public void delete(String name) {
      data.remove(name);
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
    check(new TransformedDirectory(new InMemoryDir(), new XZCompression(), new cal.prim.transforms.Encryption(Encryption.password)));
  }

  @Test
  public void testTransformOrder() throws Exception {
    EventuallyConsistentDirectory coreDir = new InMemoryDir();
    BlobTransformer compression = new XZCompression();
    BlobTransformer encryption = new cal.prim.transforms.Encryption(Encryption.password);
    EventuallyConsistentDirectory view = new TransformedDirectory(coreDir, compression, encryption);
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
