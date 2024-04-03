package cal.bkup;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.concurrent.atomic.AtomicInteger;

@Test
public class SequenceInputStreamTest {

  @Test
  @SuppressWarnings("required.method.not.called")
  public void closesArguments() throws IOException {
    AtomicInteger openStreams = new AtomicInteger(2);
    new SequenceInputStream(new TestInputStream(openStreams), new TestInputStream(openStreams)).close();
    Assert.assertEquals(openStreams.get(), 0);
  }

  private static class TestInputStream extends InputStream {
    private final AtomicInteger count;

    public TestInputStream(AtomicInteger count) {
      this.count = count;
    }

    @Override
    public int read() {
      return 0xCC;
    }

    @Override
    public void close() throws IOException {
      try {
        count.decrementAndGet();
      } finally {
        super.close();
      }
    }
  }

}
