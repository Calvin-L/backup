package cal.bkup.types;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;

public interface Rule {
  void destruct(
      IOConsumer<Path> include,
      IOConsumer<PathMatcher> exclude)
      throws IOException;
}
