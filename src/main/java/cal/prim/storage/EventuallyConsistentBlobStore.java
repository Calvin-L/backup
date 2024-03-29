package cal.prim.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

/**
 * An eventually consistent blob store.
 * Similar to {@link EventuallyConsistentDirectory}, but the store&mdash;not the client code&mdash;picks
 * the names of the blobs.  See the docstrings on {@link EventuallyConsistentDirectory} for more
 * information about what "eventually consistent" means.
 */
public interface EventuallyConsistentBlobStore {

  /**
   * The output of {@link #put(InputStream)}, containing both an {@link #identifier() identifier}
   * for the uploaded data and a {@link #bytesStored() byte count} of how many bytes got stored.
   *
   * @param identifier
   *     The identifier to retrieve the uploaded object.
   * @param bytesStored
   *     The number of bytes stored in the blob store.
   *     If you know the size of the data you sent, this
   *     number can be used as a "poor person's checksum" to
   *     verify that all of the data arrived.  If you do not,
   *     this number tells you the answer.
   */
  record PutResult(String identifier, long bytesStored) { }

  /**
   * List the entries in the directory.
   * @return
   * @throws IOException
   */
  Stream<String> list() throws IOException;

  /**
   * Create an entry.
   * The new entry might not be immediately visible to {@link #list()} or {@link #open(String)}.
   * This method never overwrites an existing entry.
   *
   * @param stream the data to write
   * @return a {@link PutResult} describing the uploaded data
   * @throws IOException if an I/O error occurs while reading the stream or writing the new entry.
   *   This exception should be treated as an <em>ambiguous</em> outcome: the entry might have been
   *   created or it might not.  Even if it was created, it might not be immediately visible to
   *   {@link #list()} or {@link #open(String)}.
   */
  PutResult put(InputStream stream) throws IOException;

  /**
   * Open an entry for reading.
   * @param name the entry to read
   * @return an unbuffered stream to read from
   * @throws IOException if the stream cannot be opened
   * @throws java.nio.file.NoSuchFileException if the entry does not exist
   */
  InputStream open(String name) throws IOException;

  /**
   * Delete an entry.
   * The change might not be immediately visible to {@link #list()} or {@link #open(String)}.
   * @param name
   * @throws IOException
   */
  void delete(String name) throws IOException;

}
