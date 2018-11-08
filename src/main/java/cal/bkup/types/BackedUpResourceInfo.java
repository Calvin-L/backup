package cal.bkup.types;

import java.time.Instant;

public interface BackedUpResourceInfo {
  BackupTarget target();
  Id idAtTarget();
  long storedSizeInBytes();
  Instant backupTime();
}
