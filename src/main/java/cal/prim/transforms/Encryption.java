package cal.prim.transforms;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class Encryption implements BlobTransformer {

  private final String key;

  public Encryption(String key) {
    this.key = key;
  }

  @Override
  public InputStream apply(InputStream data) throws IOException {
    try {
      return new EncryptedInputStream(data, key);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream unApply(InputStream data) throws IOException {
    try {
      return new DecryptedInputStream(data, key);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

}
