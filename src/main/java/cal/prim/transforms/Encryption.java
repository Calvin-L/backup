package cal.prim.transforms;

import org.checkerframework.checker.mustcall.qual.MustCallAlias;

import java.io.InputStream;

public class Encryption implements BlobTransformer {

  private final String key;

  public Encryption(String key) {
    this.key = key;
  }

  @Override
  public @MustCallAlias InputStream apply(@MustCallAlias InputStream data) {
    return new EncryptedInputStream(data, key);
  }

  @Override
  public @MustCallAlias InputStream unApply(@MustCallAlias InputStream data) {
    return new DecryptedInputStream(data, key);
  }

}
