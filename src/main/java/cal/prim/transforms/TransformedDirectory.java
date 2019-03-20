package cal.prim.transforms;

import cal.prim.EventuallyConsistentDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

public class TransformedDirectory implements EventuallyConsistentDirectory {

  private final EventuallyConsistentDirectory directory;
  private final BlobTransformer[] transforms;

  /**
   * Apply the given transformations to data placed in the directory.
   * The transformations are applied in order, so
   * <pre>
   *   new TransformedDirectory(dir, compress, encrypt)
   * </pre>
   * will produce a directory that stores the encrypted version of the
   * compressed version of the data you put into it.
   *
   * @param directory the directory to wrap
   * @param transforms the transformations to apply.  This class takes ownership of
   *                   the array, so it should not be modified after construction.
   * @return
   */
  public TransformedDirectory(EventuallyConsistentDirectory directory, BlobTransformer... transforms) {
    this.directory = directory;
    this.transforms = transforms;
  }

  @Override
  public Stream<String> list() throws IOException {
    return directory.list();
  }

  @Override
  public void createOrReplace(String name, InputStream stream) throws IOException {
    for (BlobTransformer t : transforms) {
      stream = t.apply(stream);
    }
    directory.createOrReplace(name, stream);
  }

  @Override
  public InputStream open(String name) throws IOException {
    InputStream result = directory.open(name);
    // reverse order to undo what happened in createOrReplace
    for (int i = transforms.length - 1; i >= 0; --i) {
      result = transforms[i].unApply(result);
    }
    return result;
  }

  @Override
  public void delete(String name) throws IOException {
    directory.delete(name);
  }

}
