package cal.bkup.impls;

import cal.bkup.Util;

import java.time.Duration;
import java.time.Instant;

public class RateLimitedRunnable implements Runnable {

  private final Duration rateLimit;
  private Instant lastRun;
  private final Runnable wrapped;

  public RateLimitedRunnable(Duration rateLimit, Runnable wrapped) {
    this.rateLimit = rateLimit;
    this.lastRun = null;
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
