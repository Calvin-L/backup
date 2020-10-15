package cal.prim;

import cal.bkup.Util;
import cal.prim.time.MonotonicRealTimeClock;

import java.math.BigInteger;
import java.time.Duration;

/**
 * A {@link Runnable} wrapper that restricts how often the wrapped instance
 * can run.  If it is run too often, the extra calls turn into no-ops.
 *
 * @see #run()
 */
public class RateLimitedRunnable implements Runnable {

  public enum Mode {
    RUN_ON_FIRST_CALL,
    DELAY_FIRST_RUN
  }

  private final MonotonicRealTimeClock clock;
  private final Duration rateLimit;
  private BigInteger lastRun;
  private final Runnable wrapped;

  public RateLimitedRunnable(Duration rateLimit, Mode mode, Runnable wrapped) {
    this.clock = MonotonicRealTimeClock.SYSTEM_CLOCK;
    this.rateLimit = rateLimit;
    this.lastRun = mode.equals(Mode.DELAY_FIRST_RUN) ? clock.sample() : null;
    this.wrapped = wrapped;
  }

  /**
   * Run the wrapped instance if it has not been run for the duration specified
   * in the constructor.
   */
  @Override
  public void run() {
    var now = clock.sample();
    if (lastRun == null || Util.ge(clock.timeBetweenSamples(lastRun, now), rateLimit)) {
      lastRun = now; // do this first in case wrapped.run() throws an exception
      wrapped.run();
    }
  }

}
