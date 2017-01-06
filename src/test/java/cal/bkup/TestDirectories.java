package cal.bkup;

import cal.bkup.impls.EncryptedDirectory;
import cal.bkup.impls.XZCompressedDirectory;
import cal.bkup.types.SimpleDirectory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

@Test
public class TestDirectories {

  private void check(SimpleDirectory dir) throws IOException {
    String text = "as;gihawpognaw;efe;";
    System.out.println("starting");
    try (OutputStream s = dir.createOrReplace("foo")) {
      System.out.println("started");
      s.write(text.getBytes());
      System.out.println("wrote");
//      try {
//        Thread.sleep(5000);
//      } catch (InterruptedException ignored) {
//      }
      System.out.println("closing");
    }
    System.out.println("closed");
    try (BufferedReader r = new BufferedReader(new InputStreamReader(dir.open("foo")))) {
      Assert.assertEquals(r.readLine(), text);
    }
  }

  private static class InMemoryDir implements SimpleDirectory {
    private final Map<String, byte[]> data = new HashMap<>();

    @Override
    public Stream<String> list() throws IOException {
      return data.keySet().stream();
    }

    @Override
    public OutputStream createOrReplace(String name) throws IOException {
      return new ByteArrayOutputStream() {
        @Override
        public void close() throws IOException {
          data.put(name, toByteArray());
        }
      };
    }

    @Override
    public InputStream open(String name) throws IOException {
      return new ByteArrayInputStream(data.get(name));
    }
  }

//  @Test
//  public void testBasic() throws Exception {
//    check(new InMemoryDir());
//  }
//
//  @Test
//  public void testEncrypt() throws Exception {
//    check(new EncryptedDirectory(new InMemoryDir(), Encryption.password));
//  }
//
//  @Test
//  public void testXZ() throws Exception {
//    check(new XZCompressedDirectory(new InMemoryDir()));
//  }

  @Test
  public void testXZEncrypt() throws Exception {
    check(new XZCompressedDirectory(new EncryptedDirectory(new InMemoryDir(), Encryption.password)));
  }

//  @Test
//  public void testEncryptXZ() throws Exception {
//    check(new EncryptedDirectory(new XZCompressedDirectory(new InMemoryDir()), Encryption.password));
//  }

}
