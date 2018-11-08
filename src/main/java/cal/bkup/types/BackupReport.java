package cal.bkup.types;

public interface BackupReport {
  BackupTarget target();
  byte[] sha256();
  Id idAtTarget();
  long size();
  long sizeAtTarget();
}
