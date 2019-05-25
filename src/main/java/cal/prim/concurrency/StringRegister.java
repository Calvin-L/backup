package cal.prim.concurrency;

import cal.prim.PreconditionFailed;

import java.io.IOException;

/**
 * A mutable String value.  The value is initially empty and is never null.  All implementations
 * of this interface are thread-safe.  Many implementations, like {@link DynamoDBStringRegister},
 * are backed by a persistent external resource.
 *
 * <p>Memory consistency effects: the methods on this class do not necessarily have any effect on
 * memory consistency!  Because instances of this class may be backed by an external resource,
 * concurrent calls to this class's methods might not interact via any kind of memory
 * synchronization.
 *
 * <p>This class's lack of memory consistency effects can lead to extremely subtle bugs in
 * multithreaded programs!  In particular, if thread A writes a value and thread B observes that
 * write through a call to {@link #read()} or {@link #write(String, String)}, there is <em>no
 * guarantee</em> that thread B will see any of thread A's writes to memory.  When in doubt, it
 * is always safe to lock the register before calling its methods to ensure a happens-before
 * relationship between observations made in different threads.
 */
public interface StringRegister {

  /**
   * Read the register.
   *
   * <p>In concurrent settings when the register value changes frequently, the value might already
   * have changed by the time this method returns.  Clients should be prepared for the case where
   * the returned value is already stale.  This method only promises to return some value that the
   * register had between invocation and return.
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
   * <p>In concurrent settings when the register value changes frequently, the value might already
   * have changed by the time this method returns.  Clients should be prepared for the cases where
   * (1) the write succeeds but is overwritten before this method returns, or (2) the write fails
   * but another process sets the value to <code>expectedValue</code> before this method returns.
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
