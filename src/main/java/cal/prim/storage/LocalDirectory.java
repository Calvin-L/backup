package cal.prim.storage;

import cal.bkup.Util;
import org.crashsafeio.AtomicDurableOutputStream;
import org.crashsafeio.DurableIOUtil;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class LocalDirectory implements EventuallyConsistentDirectory {

  private final Path dir;

  public LocalDirectory(Path dir) throws IOException {
    DurableIOUtil.createDirectories(dir);
    this.dir = dir;
  }

  @Override
  public Stream<String> list() throws IOException {
    List<String> result;
    try (Stream<Path> entries = Files.list(dir)) {
      result = entries.map(Path::toString).toList();
    }
    return result.stream();
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
