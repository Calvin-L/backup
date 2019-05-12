package cal.prim;

import java.io.IOException;

/**
 * A mutable String value.  The value is initially empty and is never null.  All implementations
 * of this interface are thread-safe.
 */
public interface StringRegister {

  /**
   * Read the register.
   *
   * @return the current value
   * @throws IOException if the value could not be read (for instance, because reading the
   *   value happens over a network)
   */
  String read() throws IOException;

  /**
   * Atomically set the value to <code>newValue</code> if its value is currently
   * <code>expectedValue</code> (compare-and-swap).
   *
   * @param expectedValue the expected value
   * @param newValue the new value
   * @throws NullPointerException if <code>expectedValue</code> is null or
   *    <code>newValue</code> is null
   * @throws IOException if the operation failed (for instance, because the value is stored
   *    across the network on another computer).  Clients should treat this outcome as
   *    <em>ambiguous</em>: the write may or may not have succeeded, and it may actually
   *    complete successfully <em>after</em> the <code>write()</code> call returns.
   * @throws PreconditionFailed if the current value does not equal <code>expectedValue</code>.
   */
  void write(String expectedValue, String newValue) throws IOException, PreconditionFailed;

}
