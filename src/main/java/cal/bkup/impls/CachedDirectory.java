package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.SimpleDirectory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class CachedDirectory implements SimpleDirectory {

  private final SimpleDirectory wrapped;
  private final Path cacheLocation;

  public CachedDirectory(SimpleDirectory wrapped, Path cacheLocation) {
    this.wrapped = wrapped;
    this.cacheLocation = cacheLocation;
  }

  @Override
  public Stream<String> list() throws IOException {
    return wrapped.list();
  }

  private Path find(String name) throws IOException {
    Files.createDirectories(cacheLocation);
    return cacheLocation.resolve(name);
  }

  @Override
  public OutputStream createOrReplace(String name) throws IOException {
    Path loc = find(name);
    return new FileOutputStream(loc.toString()) {
      @Override
      public void close() throws IOException {
        super.close();
        try (OutputStream copyTo = wrapped.createOrReplace(name);
             InputStream copyFrom = new FileInputStream(loc.toString())) {
          Util.copyStream(copyFrom, copyTo);
        }
      }
    };
  }

  @Override
  public InputStream open(String name) throws IOException {
    Path loc = find(name);
    if (!Files.exists(loc)) {
      try (InputStream in = wrapped.open(name);
           OutputStream out = new FileOutputStream(loc.toString())) {
        Util.copyStream(in, out);
      }
    }
    return new FileInputStream(loc.toString());
  }
}
