package cal.bkup.impls;

import cal.bkup.Util;
import cal.prim.EventuallyConsistentDirectory;
import org.crashsafeio.AtomicDurableOutputStream;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LocalDirectory implements EventuallyConsistentDirectory {

  public static final LocalDirectory TMP = new LocalDirectory(Paths.get("/tmp"));

  private final Path dir;

  public LocalDirectory(Path dir) {
    this.dir = dir;
  }

  @Override
  public Stream<String> list() throws IOException {
    return Files.list(dir).map(p -> p.getFileName().toString());
  }

  @Override
  public void createOrReplace(String name, InputStream data) throws IOException {
    try (OutputStream out = new BufferedOutputStream(new AtomicDurableOutputStream(dir.resolve(name)))) {
      Util.copyStream(data, out);
    }
  }

  @Override
  public InputStream open(String name) throws IOException {
    return new FileInputStream(dir.resolve(name).toString());
  }

  @Override
  public void delete(String name) throws IOException {
    Files.delete(dir.resolve(name));
  }

}
