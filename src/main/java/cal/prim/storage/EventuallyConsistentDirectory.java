package cal.prim.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

public interface EventuallyConsistentDirectory {

  /**
   * List the entries in the directory.
   * @return
   * @throws IOException
   */
  Stream<String> list() throws IOException;

  /**
   * Create or overwrite an entry.
   * The new entry might not be immediately visible to {@link #list()} or {@link #open(String)}.
   * @param name the name of the entry
   * @param stream the data to write
   * @throws IOException if the stream cannot be opened
   */
  void createOrReplace(String name, InputStream stream) throws IOException;

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
