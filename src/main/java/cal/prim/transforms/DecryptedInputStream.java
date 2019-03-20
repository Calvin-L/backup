package cal.prim.transforms;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class DecryptedInputStream extends FilterInputStream {

  public static byte[] decrypt(InputStream s, String password) throws IOException {
    return Util.read(new DecryptedInputStream(s, password));
  }

  public DecryptedInputStream(InputStream wrappedStream, String password) {
    super(Util.createInputStream(out -> {
      try {
        AESCrypt crypt = new AESCrypt(password);
        crypt.decrypt(wrappedStream, out);
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }));
  }

}
