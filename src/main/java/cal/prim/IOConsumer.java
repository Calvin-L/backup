package cal.prim;

import java.io.IOException;

/**
 * A {@link java.util.function.Consumer Consumer} whose {@link #accept(Object)} method
 * may throw {@link IOException}.
 * @param <T>
 */
@FunctionalInterface
public interface IOConsumer<T> {
  void accept(T x) throws IOException;
}
