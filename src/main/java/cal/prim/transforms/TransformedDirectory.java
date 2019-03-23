package cal.prim.transforms;

import cal.prim.EventuallyConsistentDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

public class TransformedDirectory implements EventuallyConsistentDirectory {

  private final EventuallyConsistentDirectory directory;
  private final BlobTransformer transform;

  /**
   * Apply the given transformations to data placed in the directory.
   *
   * @param directory the directory to wrap
   * @param transform the transformation to apply
   * @return
   */
  public TransformedDirectory(EventuallyConsistentDirectory directory, BlobTransformer transform) {
    this.directory = directory;
    this.transform = transform;
  }

  @Override
  public Stream<String> list() throws IOException {
    return directory.list();
  }

  @Override
  public void createOrReplace(String name, InputStream stream) throws IOException {
    directory.createOrReplace(name, transform.apply(stream));
  }

  @Override
  public InputStream open(String name) throws IOException {
    return transform.unApply(directory.open(name));
  }

  @Override
  public void delete(String name) throws IOException {
    directory.delete(name);
  }

}
