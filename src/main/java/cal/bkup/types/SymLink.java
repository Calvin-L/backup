package cal.bkup.types;

import java.nio.file.Path;

public interface SymLink {
  Path src();
  Path dst();
}
