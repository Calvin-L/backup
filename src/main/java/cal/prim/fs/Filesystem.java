package cal.prim.fs;

import cal.prim.IOConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Set;

public interface Filesystem {

  /**
   * Scan the filesystem, calling the appropriate consumers for each entry.
   * @param path the path to scan
   * @param exclusions a set of patterns to skip.  A path is skipped if any pattern matches its
   *                   full path or the name returned by {@link Path#getFileName()}.  If a
   *                   directory is skipped, then the contents of the directory are skipped as
   *                   well.
   * @param onSymlink a consumer for symbolic links
   * @param onFile a consumer for regular files
   * @throws IOException
   */
  void scan(Path path, Set<PathMatcher> exclusions, IOConsumer<SymLink> onSymlink, IOConsumer<RegularFile> onFile) throws IOException;

  /**
   * Open the file for reading.
   * Callers are responsible for closing the returned stream.
   * The returned stream is not buffered.
   *
   * <p>This method opens the file when called, so it may return bytes for a
   * different file than the one expected.  It may also fail if the file no
   * longer exists.  Either situation can happen if another process modifies filesystem after
   * the metadata snapshot happened but before this method was called.
   *
   * @return an open input stream
   * @throws NoSuchFileException if the file is missing
   * @throws IOException if something goes wrong when opening the file
   */
  InputStream openRegularFileForReading(Path path) throws NoSuchFileException, IOException;

}
