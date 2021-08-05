package cal.prim.transforms;

import java.io.IOException;
import java.io.InputStream;

public class TrimmedInputStream extends InputStream {

  private final InputStream wrapped;
  private long remainingBytes;

  public TrimmedInputStream(InputStream wrapped, long remainingBytes) {
    this.wrapped = wrapped;
    this.remainingBytes = remainingBytes;
  }

  @Override
  public int read() throws IOException {
    if (remainingBytes <= 0) {
      return -1;
    }
    --remainingBytes;
    return wrapped.read();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

}
