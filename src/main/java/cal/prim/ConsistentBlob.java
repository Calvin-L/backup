package cal.prim;

import java.io.IOException;
import java.io.InputStream;

/**
 * A blob that allows for compare-and-swap.
 *
 * <p>This class is similar to {@link StringRegister}, but is designed for storing very large
 * amounts of data.  It would be impractical to pass the entire data set as the expected
 * value to <code>write()</code>, so instead this class utilizes <em>tags</em>.
 *
 * <p>Its methods may throw {@link IOException} since typically the sequence will be
 * stored on disk or in the cloud.
 */
public interface ConsistentBlob {

  interface Tag {
  }

  /**
   * @return the tag for the most recent entry
   */
  Tag head() throws IOException;

  /**
   * Read an entry.  Note that, due to races, the entry returned by {@link #head()} may vanish
   * before this function can read it.  In that case the caller should retry by calling
   * {@link #head()} again.
   * @param entry a tag
   * @return an unbuffered stream to read the entry
   * @throws IOException if something goes wrong
   * @throws NoValue if no value has ever been written
   */
  InputStream read(Tag entry) throws IOException, NoValue;

  /**
   * Write a new value.
   * @param expected the entry to replace
   * @param data the data to write
   * @return the new tag
   * @throws PreconditionFailed if the blob has been changed since it had the given tag
   * @throws IOException if something goes wrong while writing.  In this case the write may or may
   *   not have actually happened, and may complete in the future.  It is safe to retry the write
   *   with the same expected tag.
   */
  Tag write(Tag expected, InputStream data) throws IOException, PreconditionFailed;

  /**
   * Delete stale data.
   * Some implementations require this to be called periodically to clean up old data.
   * @throws IOException if something goes wrong
   */
  default void cleanup() throws IOException { }

}
