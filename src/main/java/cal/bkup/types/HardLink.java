package cal.bkup.types;

import java.nio.file.Path;

public class HardLink extends Link {
  public HardLink(Path src, Path dst) {
    super(src, dst);
  }
}
