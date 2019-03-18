package cal.prim;

import java.io.IOException;
import java.io.InputStream;

/**
 * An ordered sequence of data blobs.
 * Its methods may throw {@link IOException} since typically the sequence will be
 * stored on disk or in the cloud.
 */
public interface ConsistentBlobSequence {

  /**
   * @return the sequence number for the most recent entry
   */
  long head() throws IOException;

  /**
   * Read an entry.  Note that, due to races, the entry returned by {@link #head()} may vanish
   * before this function can read it.  In that case the caller should retry by calling
   * {@link #head()} again.
   * @param entry the sequence number to read
   * @return a stream to read the entry
   * @throws IOException if something goes wrong
   */
  InputStream read(long entry) throws IOException;

  /**
   * Write the next entry in the sequence.
   * @param previousEntryNumber the entry to replace
   * @param newEntryNumber the ID of the new entry, greater than <code>previousEntryNumber</code>
   * @param data the data to write
   * @throws IllegalArgumentException if <code>newEntryNumber <= previousEntryNumber</code>
   * @throws IOException if something goes wrong while writing.  In this case the write may or may
   *   not have actually happened.  The caller should {@link #read(long)} the new entry number and
   *   verify that the data is what was expected.  If it is not present, then the write did not
   *   happen.  If it is present but not correct, then another job racing with this one won.
   */
  void writeSuccessor(long previousEntryNumber, long newEntryNumber, InputStream data) throws IOException, PreconditionFailed;

  /**
   * Delete data associated with entries older than <code>current</code>.
   * This procedure does not delete the current head or anything newer.
   * On exception, the client can safely retry.
   * @throws IOException if something goes wrong
   */
  void cleanupOldEntries(long current) throws IOException;

}
