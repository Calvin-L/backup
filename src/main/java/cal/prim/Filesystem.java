package cal.prim;

import cal.bkup.types.RegularFile;
import cal.bkup.types.SymLink;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Set;

public interface Filesystem {

  void scan(Path path, Set<PathMatcher> exclusions, IOConsumer<SymLink> onSymlink, IOConsumer<RegularFile> onFile) throws IOException;

}
