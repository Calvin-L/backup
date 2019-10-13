package cal.prim;

import java.io.FilterInputStream;
import java.io.InputStream;

/**
 * A <code>NonClosingInputStream</code> wraps another input stream, but its
 * {@link #close()} method does not close the underlying stream.  This class
 * is useful when a block of code has a stream that it needs to keep open,
 * but it needs to pass that stream to a procedure that closes its input.
 */
public class NonClosingInputStream extends FilterInputStream {

  public NonClosingInputStream(InputStream in) {
    super(in);
  }

  @Override
  public void close() {
  }

}
