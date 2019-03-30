package cal.prim.fs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;

/**
 * Information about a regular (i.e. non-link) file.
 *
 * The metadata getters {@link #path()}, {@link #modTime()}, {@link #sizeInBytes()},
 * and {@link #inode()} are pure and return information about an atomic snapshot of the file
 * at some point in the past.
 *
 * The {@link #open()} method opens the file when called, so it may return bytes for a
 * different file than the one described by the metadata.  It may also fail if the file no
 * longer exists.  Either situation can happen if another process modifies filesystem after
 * the metadata snapshot happened but before {@link #open()} was called.
 */
public interface RegularFile {
  Path path();
  Instant modTime();

  long sizeInBytes();

  /**
   * Obtain the "inode" number of the file.
   * On POSIX filesystems, this can be used to detect
   * hard links: two files that share an inode number
   * also share their contents.
   * @return the file's inode number
   */
  Object inode();

  /**
   * Open the file for reading.
   * Callers are responsible for closing the returned stream.
   * The returned stream is not buffered.
   * @return an open input stream
   * @throws NoSuchFileException if the file is missing
   * @throws IOException if something goes wrong when opening the file
   */
  InputStream open() throws NoSuchFileException, IOException;


}
