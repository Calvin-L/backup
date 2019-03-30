package cal.prim.fs;

import cal.prim.IOConsumer;

import java.io.IOException;
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

}
