package cal.prim.storage;

import lombok.Value;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.stream.Stream;

/**
 * An eventually consistent directory.  Often called an "object store", implementations of this
 * interface offer a map-like storage model that associates short string keys with large byte
 * arrays.  To avoid confusion with the Java term "Object", this interface has the name
 * "directory" instead of "object store".
 *
 * <p>Implementations of this interface are "eventually consistent", although some implementations
 * may offer stronger guarantees.  Here, eventual consistency means that:
 * <ul>
 *   <li>Modifications via {@link #createOrReplace(String, InputStream)} and
 *       {@link #delete(String)} do not perform the operation immediately; they merely schedule
 *       the operation to take place in the future.</li>
 *   <li>Queries via {@link #list()} and {@link #open(String)} may return the true state of the
 *       directory, or they may return data from any pending operation.  (Their docstrings provide
 *       more detail on what this means.)</li>
 *   <li>Every scheduled operation will eventually be applied to the directory at an unspecified
 *       time.  When multiple operations affect the same name, one of them will eventually "win"
 *       and become the true value for that name.</li>
 *   <li>There is no guarantee about which scheduled operation will win.  It is entirely possible
 *       for an earlier operation to win over a later one.</li>
 *   <li>There is no mechanism to wait for scheduled operations to complete, nor any guarantee
 *       about when they will do so.</li>
 * </ul>
 *
 * <p>These weak guarantees are hard to work with.  For instance, if there have ever been two
 * different contents associated with the same name, repeated queries may "flip-flop" on the
 * state of that entry.  You may observe the older version, then the newer version, then the
 * older version again.  Eventually, the older version may even win.  Nevertheless, with a
 * little extra help, it is still possible to build reliable systems on this abstraction.  For
 * instance, see {@link EventuallyConsistentBlobStore}.
 *
 * <p>For testing, {@link InMemoryDir} exhibits random behavior that satisfies the above
 * requirements in the most general way possible.
 */
public interface EventuallyConsistentDirectory {

  /**
   * List the entries in the directory.  The list will include all settled modifications and
   * may include any subset of the pending modifications.  Changes made while iterating over
   * the stream may or may not be included in the output.  The returned list is not guaranteed
   * to be in any particular order.  It will never contain duplicates.
   *
   * <p>Because the list may be streamed from external storage, it may throw runtime exceptions
   * during iteration.  At the moment, this interface does not define which exceptions should
   * get thrown to indicate a failure to read from external storage.
   *
   * @return a stream of entry names
   * @throws IOException if the external storage could not be reached
   */
  Stream<String> list() throws IOException;

  /**
   * Schedule the creation of an entry.
   *
   * @param name the name of the entry
   * @param stream the data to write
   * @throws IOException if the outcome of the scheduling cannot be determined
   * @throws IOException if the given <code>stream</code> throws an <code>IOException</code>
   *   while reading
   */
  void createOrReplace(String name, InputStream stream) throws IOException;

  /**
   * Open an entry for reading.  The returned data will be from the settled state of the given
   * name or from some pending operation on the given name.
   *
   * <p>If the given name is overwritten or deleted before the returned stream is closed, the
   * stream <i>may</i> throw an {@link IOException} to the reader.  However, the stream will
   * <i>never</i> return fabricated data, out-of-order data, or data from two distinct calls
   * to {@link #createOrReplace(String, InputStream) createOrReplace}.
   *
   * @param name the entry to read
   * @return an unbuffered stream to read from
   * @throws IOException if the stream cannot be opened
   * @throws NoSuchFileException if the entry does not exist
   */
  InputStream open(String name) throws IOException;

  /**
   * Schedule the deletion of an entry.
   *
   * @param name the name of the entry to delete
   * @throws IOException if the outcome of the scheduling cannot be determined
   */
  void delete(String name) throws IOException;

}
