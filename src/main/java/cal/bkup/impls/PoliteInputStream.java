package cal.bkup.impls;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * If you request bytes or skip bytes from this input stream, it always reads or skips
 * the largest amount possible.
 */
public class PoliteInputStream extends FilterInputStream {
  public PoliteInputStream(InputStream in) {
    super(in);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int nread = 0;
    while (nread < len) {
      int readThisIteration = super.read(b, off, len - nread);
      if (readThisIteration < 0) {
        return nread;
      }
      off += readThisIteration;
      nread += readThisIteration;
    }
    return len;
  }

  @Override
  public long skip(long n) throws IOException {
    while (n > 0) {
      n -= super.skip(n);
    }
    return n;
  }
}
