package cal.prim.transforms;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class DecryptedInputStream extends FilterInputStream {

  public static byte[] decrypt(InputStream s, String password) throws IOException {
    return Util.read(new DecryptedInputStream(s, password));
  }

  @SuppressWarnings("mustcallalias.out.of.scope") // TODO?
  public @MustCallAlias DecryptedInputStream(@MustCallAlias InputStream wrappedStream, String password) {
    super(Util.createInputStream(out -> {
      try (InputStream copy = wrappedStream /* ensure wrappedStream is closed */) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.decrypt(copy, out);
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }));
  }

}
