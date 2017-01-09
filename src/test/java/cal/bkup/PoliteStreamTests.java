package cal.bkup;

import cal.bkup.impls.PoliteInputStream;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;

@Test
public class PoliteStreamTests {

  private static class DerpStream extends InputStream {
    int len;

    public DerpStream(int len) {
      this.len = len;
    }

    @Override
    public int read() throws IOException {
      --len;
      return len >= 0 ? 0 : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int i = read();
      if (i < 0) return -1;
      b[off] = (byte)i;
      return 1;
    }
  }

  @Test
  public void test20() throws Exception {
    try (InputStream s = new PoliteInputStream(new DerpStream(20))) {
      Assert.assertEquals(s.read(new byte[10]), 10);
    }
  }

  @Test
  public void test10() throws Exception {
    try (InputStream s = new PoliteInputStream(new DerpStream(10))) {
      Assert.assertEquals(s.read(new byte[10]), 10);
    }
  }

  @Test
  public void test5() throws Exception {
    try (InputStream s = new PoliteInputStream(new DerpStream(5))) {
      Assert.assertEquals(s.read(new byte[10]), 5);
    }
  }

  @Test
  public void test0() throws Exception {
    try (InputStream s = new PoliteInputStream(new DerpStream(0))) {
      Assert.assertEquals(s.read(new byte[10]), -1);
    }
  }

}
