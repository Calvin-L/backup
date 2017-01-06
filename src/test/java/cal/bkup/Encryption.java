package cal.bkup;

import cal.bkup.impls.DecryptedInputStream;
import cal.bkup.impls.EncryptedOutputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.Arrays;
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

}
