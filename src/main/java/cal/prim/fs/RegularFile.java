package cal.prim.fs;

import lombok.NonNull;
import lombok.Value;
import lombok.experimental.NonFinal;

import javax.annotation.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Information about a regular (i.e. non-link) file.
 *
 * The metadata getters {@link #getPath()}, {@link #getModTime()}, {@link #getSizeInBytes()},
 * and {@link #getINode()} are pure and return information about an atomic snapshot of the file
 * at some point in the past.
 *
 * The {@link #open()} method opens the file when called, so it may return bytes for a
 * different file than the one described by the metadata.  It may also fail if the file no
 * longer exists.  Either situation can happen if another process modifies filesystem after
 * the metadata snapshot happened but before {@link #open()} was called.
 */
@Value
@NonFinal // allow tests to override open()
public class RegularFile {
  @NonNull Path path;
  @NonNull Instant modTime;
  long sizeInBytes;

  /**
   * The "inode" number of the file.
   * On POSIX filesystems, this can be used to detect
   * hard links: two files that share an inode number
   * also share their contents.
   */
  @Nullable Object iNode;

  /**
   * Open the file for reading.
   * Callers are responsible for closing the returned stream.
   * The returned stream is not buffered.
   * @return an open input stream
   * @throws NoSuchFileException if the file is missing
   * @throws IOException if something goes wrong when opening the file
   */
  public InputStream open() throws NoSuchFileException, IOException {
    try {
      return Files.newInputStream(path);
    } catch (FileNotFoundException e) {
      // The JavaDoc for Files.newInputStream() does not specify which of the standard
      // library's two "file is missing" exceptions get thrown.  Since it belongs to the
      // `java.nio` package I suspect it will always throw NoSuchFileException, but just
      // in case, this block catches the other one and re-throws it as the one specified
      // by the JavaDoc contract for RegularFile.open().
      throw new NoSuchFileException(path.toString());
    }
  }

}
