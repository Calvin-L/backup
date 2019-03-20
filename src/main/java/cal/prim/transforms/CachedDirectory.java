package cal.prim.transforms;

import cal.bkup.Util;
import cal.prim.EventuallyConsistentDirectory;
import org.crashsafeio.AtomicDurableOutputStream;
import org.crashsafeio.DurableIOUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class CachedDirectory implements EventuallyConsistentDirectory {

  private final EventuallyConsistentDirectory wrapped;
  private final Path cacheLocation;

  public CachedDirectory(EventuallyConsistentDirectory wrapped, Path cacheLocation) throws IOException {
    this.wrapped = wrapped;
    this.cacheLocation = cacheLocation;
    DurableIOUtil.createFolders(cacheLocation);
  }

  @Override
  public Stream<String> list() throws IOException {
    return wrapped.list();
  }

  private Path find(String name) {
    return cacheLocation.resolve(name);
  }

  @Override
  public synchronized void createOrReplace(String name, InputStream data) throws IOException {
    // TODO: improve performance by locking the name, not the whole directory
    // The lock is necessary so that the data doesn't get clobbered between writing to the cache
    // and writing to the wrapped directory.
    Path loc = find(name);
    try (OutputStream out = new BufferedOutputStream(new AtomicDurableOutputStream(loc))) {
      Util.copyStream(data, out);
    }
    try (InputStream in = new BufferedInputStream(Files.newInputStream(loc))) {
      wrapped.createOrReplace(name, in);
    }
  }

  @Override
  public InputStream open(String name) throws IOException {
    Path loc = find(name);
    if (!Files.exists(loc)) {
      System.out.println("Caching " + name + " as " + loc);
      try (InputStream in = wrapped.open(name);
           OutputStream out = new FileOutputStream(loc.toString())) {
        Util.copyStream(in, out);
      }
    }
    return new FileInputStream(loc.toString());
  }

  @Override
  public void delete(String name) throws IOException {
    Files.delete(find(name));
  }

}
