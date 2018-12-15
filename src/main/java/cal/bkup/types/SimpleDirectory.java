package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public interface SimpleDirectory {

  /**
   * List all the entries in the directory.
   * @return
   * @throws IOException
   */
  Stream<String> list() throws IOException;

  /**
   * Open an output stream to create or overwrite
   * an entry.
   * The output stream must be <i>atomic</i> and
   * <i>durable</i>:
   * {@link OutputStream#close()} either creates
   * the entry in its entirety (persistently
   * saved to disk or other media) or does nothing.
   * @param name the name of the entry
   * @return an output stream to write to
   * @throws IOException if the stream cannot be opened
   */
  OutputStream createOrReplace(String name) throws IOException;

  /**
   * Open an output stream to create an entry.
   * Identical to {@link #createOrReplace(String)}, but
   * the stream cannot be closed successfully if
   * another entry exists with the same name.
   * @param name the name of the entry
   * @return an output stream to write to
   * @throws IOException if the stream cannot be opened
   */
  OutputStream create(String name) throws IOException;

  /**
   * Open an entry for reading.
   * @param name the entry to read
   * @return a stream to read from
   * @throws IOException if the stream cannot be opened
   * @throws java.nio.file.NoSuchFileException if the entry does not exist
   */
  InputStream open(String name) throws IOException;

}
