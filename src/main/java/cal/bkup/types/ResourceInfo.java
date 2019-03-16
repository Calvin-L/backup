package cal.bkup.types;

import java.nio.file.Path;
import java.time.Instant;

public interface ResourceInfo {
  Id system();
  Path path();
  Instant modTime();
  Id target();
  Id idAtTarget();
  long sizeAtTarget();
  Sha256AndSize trueSummary();
}
