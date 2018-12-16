package cal.bkup.types;

public interface BackupReport {
  /**
   * @return the identifier assigned to the resource by the target
   */
  Id idAtTarget();

  /**
   * Determine how many bytes were actually stored.
   * The backup target might perform transformations on the data
   * (such as compression or encryption).
   * This method determines how many bytes were actually stored AFTER
   * transformations.
   * @return the number of bytes stored by the backup target to contain the data
   */
  long sizeAtTarget();
}
