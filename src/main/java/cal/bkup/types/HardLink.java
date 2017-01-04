package cal.bkup.types;

import java.nio.file.Path;

public interface HardLink {
  Path src();
  Path dst();
}
