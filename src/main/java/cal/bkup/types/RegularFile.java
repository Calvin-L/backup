package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;

public interface RegularFile {
  Path path();
  Instant modTime();

  /**
   * Open the file for reading.
   * Callers are responsible for closing the returned stream.
   * The returned stream is not buffered.
   * @return an open input stream
   * @throws NoSuchFileException if the file is missing
   * @throws IOException if something goes wrong when opening the file
   */
  InputStream open() throws NoSuchFileException, IOException;

  long sizeEstimateInBytes();

  /**
   * Obtain the "inode" number of the file.
   * On POSIX filesystems, this can be used to detect
   * hard links: two files that share an inode number
   * also share their contents.
   * @return the file's inode number
   */
  Object inode();

}
