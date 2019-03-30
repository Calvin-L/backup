package cal.prim.fs;

import java.nio.file.Path;

public class HardLink extends Link {
  public HardLink(Path src, Path dst) {
    super(src, dst);
  }
}
