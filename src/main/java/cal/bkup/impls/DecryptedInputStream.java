package cal.bkup.impls;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class DecryptedInputStream extends FilterInputStream {

  public static byte[] decrypt(InputStream s, String password) throws IOException, GeneralSecurityException {
    byte[] bytes = Util.read(s);
    try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
         ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      AESCrypt crypt = new AESCrypt(password);
      crypt.decrypt(bytes.length, in, out);
      return out.toByteArray();
    }
  }

  public DecryptedInputStream(InputStream wrappedStream, String password) throws IOException, GeneralSecurityException {
    super(new ByteArrayInputStream(decrypt(wrappedStream, password)));
  }

}
