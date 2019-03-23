package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

public interface RegularFile {
  Path path();
  Instant modTime();
  InputStream open() throws IOException;
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
