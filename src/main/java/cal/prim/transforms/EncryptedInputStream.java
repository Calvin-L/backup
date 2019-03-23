package cal.prim.transforms;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class EncryptedInputStream extends FilterInputStream {

  public static final int AES_VERSION = 2;

  public EncryptedInputStream(InputStream wrappedStream, String password) {
    super(Util.createInputStream(out -> {
      try (InputStream copy = wrappedStream /* ensure wrappedStream is closed */) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.encrypt(AES_VERSION, copy, out);
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    }));
  }

}
