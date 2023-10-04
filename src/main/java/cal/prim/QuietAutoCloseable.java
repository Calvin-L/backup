package cal.prim;

import org.checkerframework.checker.mustcall.qual.InheritableMustCall;

/**
 * An <code>AutoCloseable</code> object whose {@link #close()} method does not throw
 * any checked exceptions.
 */
@InheritableMustCall("close")
public interface QuietAutoCloseable extends AutoCloseable {
  @Override
  void close();
}
