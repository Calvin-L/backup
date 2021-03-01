package cal.prim.storage;

import cal.bkup.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class ConsistentInMemoryDir implements EventuallyConsistentDirectory {

  private final Map<String, byte[]> entries = new HashMap<>();

  @Override
  public synchronized Stream<String> list() {
    return new ArrayList<>(entries.keySet()).stream();
  }

  @Override
  public synchronized void createOrReplace(String name, InputStream stream) throws IOException {
    entries.put(name, Util.read(stream));
  }

  @Override
  public InputStream open(String name) throws NoSuchFileException {
    byte[] data;
    synchronized (this) {
      data = entries.get(name);
    }
    if (data == null) {
      throw new NoSuchFileException(name);
    }
    return new ByteArrayInputStream(data);
  }

  @Override
  public void delete(String name) {
    entries.remove(name);
  }

  @Override
  public String toString() {
    return entries.toString();
  }

}
