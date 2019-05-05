package cal.prim;

import cal.prim.transforms.StatisticsCollectingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.stream.Stream;

public class BlobStoreOnDirectory implements EventuallyConsistentBlobStore {

  private final EventuallyConsistentDirectory data;

  public BlobStoreOnDirectory(EventuallyConsistentDirectory data) {
    this.data = data;
  }

  @Override
  public Stream<String> list() throws IOException {
    return data.list();
  }

  @Override
  public PutResult put(InputStream stream) throws IOException {
    String id = UUID.randomUUID().toString();
    StatisticsCollectingInputStream in = new StatisticsCollectingInputStream(stream, (x) -> { });
    data.createOrReplace(id, in);
    return new PutResult(id, in.getBytesRead());
  }

  @Override
  public InputStream open(String name) throws IOException {
    return data.open(name);
  }

  @Override
  public void delete(String name) throws IOException {
    data.delete(name);
  }

}
