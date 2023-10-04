package cal.prim.transforms;

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.checkerframework.checker.mustcall.qual.Owning;

import java.io.IOException;
import java.io.InputStream;

public class TrimmedInputStream extends InputStream {

  private final @Owning InputStream wrapped;
  private long remainingBytes;

  public @MustCallAlias TrimmedInputStream(@MustCallAlias InputStream wrapped, long remainingBytes) {
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
  @EnsuresCalledMethods(value = "wrapped", methods = {"close"})
  public void close() throws IOException {
    wrapped.close();
  }

}
