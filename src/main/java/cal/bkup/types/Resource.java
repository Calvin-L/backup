package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;

public interface Resource {
  Id system();
  Path path();
  Instant modTime() throws IOException;
  InputStream open() throws IOException;
  long sizeEstimateInBytes() throws IOException;
}
