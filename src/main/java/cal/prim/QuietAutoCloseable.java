package cal.prim;

/**
 * An <code>AutoCloseable</code> object whose {@link #close()} method does not throw
 * any checked exceptions.
 */
public interface QuietAutoCloseable extends AutoCloseable {
  @Override
  void close();
}
