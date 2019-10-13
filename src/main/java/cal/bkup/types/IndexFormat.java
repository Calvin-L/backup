package cal.bkup.types;

import cal.bkup.impls.BackupIndex;
import cal.prim.MalformedDataException;

import java.io.IOException;
import java.io.InputStream;

/**
 * An <code>IndexFormat</code> implements serialization and deserialization
 * for the {@link BackupIndex} class.
 *
 * @see #load(InputStream)
 * @see #serialize(BackupIndex)
 */
public interface IndexFormat {

  /**
   * Load an index from an input stream.  This method does not close the
   * stream; the caller is responsible instead.
   *
   * <p>Performance note: implementations assume that the input stream
   * offers good performance for small reads.  Callers should generally
   * wrap their input in {@link java.io.BufferedInputStream} before
   * calling this method.
   *
   * @param data a stream of bytes
   * @return a deserialized backup index
   * @throws IOException if a problem occurs while reading from the stream
   * @throws MalformedDataException if the stream contains malformed data
   */
  BackupIndex load(InputStream data) throws IOException, MalformedDataException;

  /**
   * Serialize an index.  The returned input stream can be used to read the
   * serialized bytes.  Callers must not modify the index until the returned
   * stream has been closed.
   *
   * @param index an index
   * @return a stream of bytes
   */
  InputStream serialize(BackupIndex index);

}
