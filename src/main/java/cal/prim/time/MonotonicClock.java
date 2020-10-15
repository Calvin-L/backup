package cal.prim.time;

import java.math.BigInteger;

/**
 * A monotonic clock is one that never decreases.  All implementations of this
 * interface are thread safe.
 */
public interface MonotonicClock {

  /**
   * Sample (i.e. read) the clock.  The result may be negative, depending
   * on the implementation of the clock.  However, later samples will never
   * be less than earlier samples.
   *
   * @return a sample
   */
  BigInteger sample();

}
