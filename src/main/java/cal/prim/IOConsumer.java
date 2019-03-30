package cal.prim;

import java.io.IOException;

@FunctionalInterface
public interface IOConsumer<T> {
  void accept(T x) throws IOException;
}
