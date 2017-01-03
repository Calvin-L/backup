package cal.bkup.types;

import java.io.IOException;

@FunctionalInterface
public interface IOConsumer<T> {
  void accept(T x) throws IOException;
}
