package cal.bkup;

import org.testng.annotations.Test;

@Test
public class AWS {

  @Test
  public void testCredentials() {
    AWSTools.getCredentials();
  }

}
