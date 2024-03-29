package cal.bkup;

import cal.bkup.types.Sha256AndSize;
import cal.prim.fs.HardLink;
import cal.prim.fs.Link;
import cal.prim.fs.SymLink;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

@Test
public class UtilTests {

  private final String SHA256_FOR_ZERO_BYTES = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  @Test
  public void testSha256() throws IOException {
    byte[] data = new byte[0];
    byte[] sha256bytes = Util.sha256(new ByteArrayInputStream(data));
    String sha256string = Util.sha256toString(sha256bytes);
    Assert.assertEquals(sha256string, SHA256_FOR_ZERO_BYTES);
    byte[] sha256bytes2 = Util.stringToSha256(sha256string);
    Assert.assertEquals(sha256bytes2,  sha256bytes);
  }

  /**
   * The {@link Sha256AndSize} class contains a byte array member.
   * This test ensures that its equality compares byte arrays by
   * value, not by reference.
   */
  @Test
  public void testSha256AndSizeEquality() {
    Random r = new Random(33);

    byte[] a = new byte[32];
    r.nextBytes(a);

    Sha256AndSize x = new Sha256AndSize(a, 101L);
    Sha256AndSize y = new Sha256AndSize(Arrays.copyOf(a, a.length), 101L);
    Assert.assertEquals(x, y);
  }

  /**
   * This test ensures that the {@link Link} class has correct
   * equality comparisons for its subclasses {@link HardLink}
   * and {@link SymLink}.
   */
  @Test
  public void testLinkEquals() {
    Link hl1 = new HardLink(Paths.get("a"), Paths.get("b"));
    Link hl2 = new HardLink(Paths.get("a"), Paths.get("b"));
    Assert.assertEquals(hl1, hl2);

    Link sl1 = new SymLink(Paths.get("a"), Paths.get("b"));
    Link sl2 = new SymLink(Paths.get("a"), Paths.get("b"));
    Assert.assertEquals(sl1, sl2);

    Assert.assertNotEquals(hl1, sl1);
  }

}
