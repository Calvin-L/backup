package cal.bkup.types;

import java.time.Instant;

public interface BackedUpResourceInfo {
  Id idAtTarget();
  long sizeInBytes();
  Instant backupTime();
}
