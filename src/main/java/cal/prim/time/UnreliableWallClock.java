package cal.prim.time;

import java.time.Instant;

/**
 * A wall clock can tell you the date and time.  Most ways of measuring wall
 * clock time (such as Java's <code>Instant.now()</code>) are unreliable: a
 * computer's notion of wall clock time can be wrong.  It can also change in
 * unexpected ways, with sudden jumps forward and backward.
 *
 * <p>Be aware that:
 * <ul>
 *   <li>Reported wall clock times can be wrong.</li>
 *   <li>Differences between two sampled wall clock times do not necessarily
 *       correspond to the real duration between those samples.  If you need
 *       reliable duration measurements, use {@link MonotonicRealTimeClock}.
 *       </li>
 * </ul>
 */
public interface UnreliableWallClock {
  Instant now();

  UnreliableWallClock SYSTEM_CLOCK = Instant::now;
}
