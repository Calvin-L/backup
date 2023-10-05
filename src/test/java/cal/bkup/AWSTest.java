package cal.bkup;

import org.testng.annotations.Test;

@Test
public class AWSTest {

  @Test
  public void testCredentials() {
    try (var provider = AWSTools.credentialsProvider()) {
      provider.resolveCredentials();
    }
  }

}
