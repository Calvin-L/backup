package cal.bkup.impls;

import cal.bkup.types.SimpleDirectory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class LocalDirectory implements SimpleDirectory {

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
  public OutputStream createOrReplace(String name) throws IOException {
    return new FileOutputStream(dir.resolve(name).toString());
  }

  @Override
  public InputStream open(String name) throws IOException {
    return new FileInputStream(dir.resolve(name).toString());
  }

}
