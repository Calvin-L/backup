package cal.bkup.types;

import java.nio.file.Path;

public class SymLink extends Link {
  public SymLink(Path src, Path dst) {
    super(src, dst);
  }
}
