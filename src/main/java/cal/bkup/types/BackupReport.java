package cal.bkup.types;

/**
 * Information about a backed-up file.
 *
 * @param  idAtTarget
 *     The identifier for the file contents in the target store.
 * @param offsetAtTarget
 *     The offset, in bytes, of the file contents within the target store
 *     entity named by {@link #idAtTarget}.
 * @param sizeAtTarget
 *     The number of bytes stored by the backup target to contain the data.
 *     The backup target might perform transformations on the data
 *     (such as compression or encryption).
 *     This method determines how many bytes were actually stored AFTER
 *     transformations.
 * @param key
 *     The key that encrypts the data in the target store.
 *
 * @see cal.bkup.impls.BackupIndex#addBackedUpBlob(Sha256AndSize, BackupReport)
 * @see cal.bkup.impls.BackupIndex#lookupBlob(Sha256AndSize)
 */
public record BackupReport(
    String idAtTarget,
    long offsetAtTarget,
    long sizeAtTarget,
    String key) {
}
