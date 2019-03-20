package cal.prim.transforms;

import java.io.InputStream;

public class Encryption implements BlobTransformer {

  private final String key;

  public Encryption(String key) {
    this.key = key;
  }

  @Override
  public InputStream apply(InputStream data) {
    return new EncryptedInputStream(data, key);
  }

  @Override
  public InputStream unApply(InputStream data) {
    return new DecryptedInputStream(data, key);
  }

}
