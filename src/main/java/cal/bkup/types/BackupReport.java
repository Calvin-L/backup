package cal.bkup.types;

import lombok.NonNull;
import lombok.Value;

/**
 * Information about a backed-up file.
 *
 * @see cal.bkup.impls.BackupIndex#addBackedUpBlob(Sha256AndSize, BackupReport)
 * @see cal.bkup.impls.BackupIndex#lookupBlob(Sha256AndSize)
 */
@Value
public class BackupReport {
  /**
   * The identifier for the file contents in the target store.
   */
  @NonNull String idAtTarget;

  /**
   * The offset, in bytes, of the file contents within the target store
   * entity named by {@link #idAtTarget}.
   */
  long offsetAtAtTarget;

  /**
   * The number of bytes stored by the backup target to contain the data.
   * The backup target might perform transformations on the data
   * (such as compression or encryption).
   * This method determines how many bytes were actually stored AFTER
   * transformations.
   */
  long sizeAtTarget;

  /**
   * The key that encrypts the data in the target store.
   */
  @NonNull String key;
}
