package cal.prim.time;

import java.math.BigInteger;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * A monotonic clock with some correspondence to real time.  The wall clock
 * time associated with clock samples is not knowable, but differences between
 * samples reliably measure elapsed time with only a few percentage points of
 * error.
 *
 * @see #SYSTEM_CLOCK
 * @see #timeBetweenSamples(BigInteger, BigInteger)
 */
public interface MonotonicRealTimeClock extends MonotonicClock {

  /**
   * Determine how much time elapsed between the first sample and the second.
   * The result is positive if the first sample came before the second, and
   * negative if the second sample came before the first.  Note that the
   * result is approximate; it is likely to be accurate to within a few
   * percentage points, but it is not exact.
   *
   * @param start the first sample
   * @param end the second sample
   * @return the approximate real-time duration between the samples
   */
  Duration timeBetweenSamples(BigInteger start, BigInteger end);

  /**
   * The default real time clock, backed by {@link System#nanoTime()}.  This
   * is the only "true" implementation of the interface, although other
   * implementations may exist for testing.
   */
  MonotonicRealTimeClock SYSTEM_CLOCK = new MonotonicRealTimeClock() {
    BigInteger n = BigInteger.ZERO;
    long prev = 0;

    @Override
    public synchronized BigInteger sample() {
      long newTime = System.nanoTime();
      long elapsedNanos = Math.subtractExact(newTime, prev);
      prev = newTime;
      n = n.add(BigInteger.valueOf(elapsedNanos));
      return n;
    }

    @Override
    public Duration timeBetweenSamples(BigInteger start, BigInteger end) {
      long elapsed = end.subtract(start).longValueExact();
      return Duration.of(elapsed, ChronoUnit.NANOS);
    }
  };

}
