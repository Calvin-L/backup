package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;

public interface Resource {
  Path path();
  Instant modTime() throws IOException;
  InputStream open() throws IOException;
}
