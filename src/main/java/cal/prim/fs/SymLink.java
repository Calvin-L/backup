package cal.prim.fs;

import java.nio.file.Path;

public record SymLink(Path source, Path destination) implements Link {
}
