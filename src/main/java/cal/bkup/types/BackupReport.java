package cal.bkup.types;

public interface BackupReport {
  BackupTarget target(); // TODO: remove this
  Id idAtTarget();

  /**
   * @return the SHA-256 digest of the backed-up data
   */
  byte[] sha256();

  /**
   * @return the number of bytes in the backed-up data
   * @see #sizeAtTarget()
   */
  long size();

  /**
   * Determine how many bytes were actually stored.
   * The backup target might perform transformations on the data
   * (such as compression or encryption).
   * While {@link #size()} determines how many bytes are in the data,
   * this method determines how many bytes were actually stored after
   * transformations.
   * @return the number of bytes stored by the backup target to contain the data
   */
  long sizeAtTarget();
}
