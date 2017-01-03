package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public interface SimpleDirectory {
  Stream<String> list() throws IOException;
  OutputStream createOrReplace(String name) throws IOException;
  InputStream open(String name) throws IOException;
}
