package cal.prim.fs;

import java.nio.file.Path;

public class SymLink extends Link {
  public SymLink(Path src, Path dst) {
    super(src, dst);
  }
}
