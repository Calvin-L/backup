package cal.bkup;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

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

}
