package cal.bkup.types;

import java.nio.file.Path;

public interface Link {
  Path src();
  Path dst();
}
