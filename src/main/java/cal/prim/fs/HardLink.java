package cal.prim.fs;

import java.nio.file.Path;

public record HardLink(Path source, Path destination) implements Link {
}
