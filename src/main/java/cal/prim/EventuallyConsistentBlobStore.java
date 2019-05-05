package cal.prim;

import lombok.Value;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

/**
 * A blob store.
 * Similar to {@link EventuallyConsistentDirectory}, but the store&mdash;not the client code&mdash;picks
 * the names of the blobs.
 */
public interface EventuallyConsistentBlobStore {

  @Value
  class PutResult {
    /**
     * The identifier to retrieve the uploaded object.
     */
    String identifier;

    /**
     * The number of bytes stored in the blob store.
     * If you know the size of the data you sent, this
     * number can be used as a "poor person's checksum" to
     * verify that all of the data arrived.  If you do not,
     * this number tells you the answer.
     */
    long bytesStored;
  }

  /**
   * List the entries in the directory.
   * @return
   * @throws IOException
   */
  Stream<String> list() throws IOException;

  /**
   * Create an entry.
   * The new entry might not be immediately visible to {@link #list()} or {@link #open(String)}.
   * @param stream the data to write
   * @return a {@link PutResult} describing the uploaded data
   * @throws IOException if the stream cannot be opened
   */
  PutResult put(InputStream stream) throws IOException;

  /**
   * Open an entry for reading.
   * @param name the entry to read
   * @return a stream to read from
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
