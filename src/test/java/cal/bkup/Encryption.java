package cal.bkup;

import cal.bkup.impls.DecryptedInputStream;
import cal.bkup.impls.EncryptedInputStream;
import cal.bkup.impls.EncryptedOutputStream;
import com.amazonaws.util.StringInputStream;
import es.vocali.util.AESCrypt;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

@Test
public class Encryption {

  public static final String password = "yes hello this is dog";

  static void convertAndDeconvert(
      String text,
      Function<InputStream, InputStream> inProvider,
      Function<OutputStream, OutputStream> outProvider) throws IOException {

    byte[] inBytes = text.getBytes();

    byte[] bytes;
    ByteArrayOutputStream s = new ByteArrayOutputStream();
    try (OutputStream out = outProvider.apply(s)) {
      Util.copyStream(new ByteArrayInputStream(inBytes), out);
    }
    bytes = s.toByteArray();

    System.out.println("intermediate: " + Arrays.toString(bytes));

    byte[] finalBytes;
    try (InputStream in = inProvider.apply(new ByteArrayInputStream(bytes))) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Util.copyStream(in, out);
      finalBytes = out.toByteArray();
    }

    Assert.assertEquals(new String(finalBytes), text);
  }

  private void check(String text) throws IOException {
    convertAndDeconvert(text,
        (s) -> {
          try {
            return new DecryptedInputStream(s, password);
          } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException(e);
          }
        },
        (s) -> {
          try {
            return new EncryptedOutputStream(s, password);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testEmptyString() throws Exception {
    check("");
  }

  @Test
  public void testFoo() throws Exception {
    check("foo");
  }

  @Test
  public void testPassword() throws Exception {
    check(password);
  }

  @Test
  public void testLong() throws Exception {
    check("aosfha;efhaw;ofn;awegb;awibg;awoehij;awoeijfd;awfi;oawehfgaow;e;aewg");
  }

  @Test
  public void testEncryptedInputStream1() throws Exception {
    Random random = new Random();
    for (int i = 0; i <= 32; ++i) {
      System.out.print("len=" + i + "...");
      System.out.flush();
      byte[] bytesExpected = new byte[i];
      random.nextBytes(bytesExpected);
      byte[] bytesOut = DecryptedInputStream.decrypt(
          new EncryptedInputStream(new SlowStream(new ByteArrayInputStream(bytesExpected)), password),
          password);
      if (!Arrays.equals(bytesOut, bytesExpected)) {
        System.out.println(Arrays.toString(bytesOut) + " != " + Arrays.toString(bytesExpected));
        Assert.assertEquals(bytesOut, bytesExpected);
      } else {
        System.out.println(" ok");
      }
    };
  }

  @Test
  public void testSlowStreams() throws Exception {
    String data = "hello world";
    String password = "asdfjkl;";
    String charset = "UTF-8";

    // Encrypt
    ByteArrayOutputStream encryptedOut = new ByteArrayOutputStream();
    new AESCrypt(password).encrypt(2, new SlowStream(new ByteArrayInputStream(data.getBytes(charset))), encryptedOut);
    byte[] encrypted = encryptedOut.toByteArray();

    // Decrypt
    ByteArrayOutputStream decryptedOut = new ByteArrayOutputStream();
    new AESCrypt(password).decrypt(encrypted.length, new SlowStream(new ByteArrayInputStream(encrypted)), decryptedOut);
    String decrypted = new String(decryptedOut.toByteArray(), charset);

    Assert.assertEquals(data, decrypted);
  }

  @Test
  public void testEncryptedInputStream2() throws Exception {
    String data = "hello world!";
    do {
      Assert.assertEquals(
          new String(DecryptedInputStream.decrypt(
              new EncryptedInputStream(new SlowStream(new StringInputStream(data)), password),
              password)),
          data);
      data = data + data;
    } while (data.length() < 1024 * 1024 * 2);
  }

  private class SlowStream extends FilterInputStream {
    public SlowStream(InputStream stream) { super(stream); }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return super.read(b, off, Math.min(len, 1));
    }

    @Override
    public long skip(long n) throws IOException {
      return super.skip(1);
    }
  }
}
