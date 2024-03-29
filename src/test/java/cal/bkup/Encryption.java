package cal.bkup;

import cal.prim.transforms.DecryptedInputStream;
import cal.prim.transforms.EncryptedInputStream;
import es.vocali.util.AESCrypt;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;

@Test
public class Encryption {

  public static final String password = "yes hello this is dog";

  static void convertAndDeconvert(
      String text,
      Function<@MustCall({}) InputStream, @MustCall({}) InputStream> inProvider,
      Function<@MustCall({}) InputStream, @MustCall({}) InputStream> outProvider) throws IOException {

    byte[] inBytes = text.getBytes();

    byte[] bytes;
    try (InputStream in = outProvider.apply(new ByteArrayInputStream(inBytes))) {
      bytes = Util.read(in);
    }

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
        (s) -> new DecryptedInputStream(s, password),
        (s) -> new EncryptedInputStream(s, password));
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
    new AESCrypt(password).decrypt(new SlowStream(new ByteArrayInputStream(encrypted)), decryptedOut);
    String decrypted = new String(decryptedOut.toByteArray(), charset);

    Assert.assertEquals(data, decrypted);
  }

  @Test
  public void testEncryptedInputStream2() throws Exception {
    String data = "hello world!";
    do {
      Assert.assertEquals(
          new String(DecryptedInputStream.decrypt(
              new EncryptedInputStream(new SlowStream(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8))), password),
              password)),
          data);
      data = data + data;
    } while (data.length() < 1024 * 1024 * 2);
  }

  private static class SlowStream extends FilterInputStream {
    public @MustCallAlias SlowStream(@MustCallAlias InputStream stream) { super(stream); }

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
