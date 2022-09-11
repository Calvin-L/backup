package cal.prim.fs;

import java.nio.file.Path;

public sealed interface Link permits SymLink, HardLink {
  Path source();
  Path destination();
}
