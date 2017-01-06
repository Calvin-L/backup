package cal.bkup;

import org.testng.annotations.Test;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;

import static cal.bkup.Encryption.convertAndDeconvert;

@Test
public class XZCompression {

  private void check(String text) throws IOException {
    convertAndDeconvert(text,
        (s) -> {
          try {
            return new XZInputStream(s);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        },
        (s) -> {
          try {
            return new XZOutputStream(s, new LZMA2Options());
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
  public void testLong() throws Exception {
    check("aosfha;efhaw;ofn;awegb;awibg;awoehij;awoeijfd;awfi;oawehfgaow;e;aewg");
  }

}
