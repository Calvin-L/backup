package cal.prim;

import cal.bkup.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class InMemoryDir implements EventuallyConsistentDirectory {
  private final Map<String, byte[]> data = new HashMap<>();

  @Override
  public Stream<String> list() {
    return data.keySet().stream();
  }

  @Override
  public void createOrReplace(String name, InputStream s) throws IOException {
    byte[] bytes = Util.read(s);
    System.out.println("dir[" + name + "] = " + Arrays.toString(bytes));
    data.put(name, bytes);
  }

  @Override
  public InputStream open(String name) {
    return new ByteArrayInputStream(data.get(name));
  }

  @Override
  public void delete(String name) {
    data.remove(name);
  }
}
