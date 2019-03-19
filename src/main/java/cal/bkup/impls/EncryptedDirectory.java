package cal.bkup.impls;

import cal.prim.SimpleDirectory;
import cal.prim.transforms.DecryptedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.util.stream.Stream;

public class EncryptedDirectory implements SimpleDirectory {

  private final SimpleDirectory wrapped;
  private final String password;

  public EncryptedDirectory(SimpleDirectory wrapped, String password) {
    this.wrapped = wrapped;
    this.password = password;
  }

  @Override
  public Stream<String> list() throws IOException {
    return wrapped.list();
  }

  @Override
  public OutputStream createOrReplace(String name) throws IOException {
    return new EncryptedOutputStream(wrapped.createOrReplace(name), password);
  }

  @Override
  public InputStream open(String name) throws IOException {
    try {
      return new DecryptedInputStream(wrapped.open(name), password);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

}
