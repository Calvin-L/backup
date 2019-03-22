package cal.prim;

import cal.bkup.Util;

import java.time.Duration;
import java.time.Instant;

public class RateLimitedRunnable implements Runnable {

  public enum Mode {
    RUN_ON_FIRST_CALL,
    DELAY_FIRST_RUN
  }

  private final Duration rateLimit;
  private Instant lastRun;
  private final Runnable wrapped;

  public RateLimitedRunnable(Duration rateLimit, Mode mode, Runnable wrapped) {
    this.rateLimit = rateLimit;
    this.lastRun = mode.equals(Mode.DELAY_FIRST_RUN) ? Instant.now() : null;
    this.wrapped = wrapped;
  }

  @Override
  public void run() {
    Instant now = Instant.now();
    if (lastRun == null || Util.ge(Duration.between(lastRun, now), rateLimit)) {
      lastRun = now; // do this first in case wrapped.run() throws an exception
      wrapped.run();
    }
  }

}
