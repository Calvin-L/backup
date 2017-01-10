package cal.bkup.impls;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;

public class DecryptedInputStream extends FilterInputStream {

  public static byte[] decrypt(InputStream s, String password) throws IOException, GeneralSecurityException {
    return Util.read(new DecryptedInputStream(s, password));
  }

  private final Thread decryptionThread;
  private final AtomicReference<Throwable> fail = new AtomicReference<>(null);

  public DecryptedInputStream(InputStream wrappedStream, String password) throws GeneralSecurityException, IOException {
    super(new PipedInputStream(4096));
    PipedOutputStream out = new PipedOutputStream((PipedInputStream)in);
    decryptionThread = new Thread(() -> {
      try (PipedOutputStream cpy = out; /* ensure that "out" gets closed */
           InputStream in = wrappedStream /* ensure that "wrappedStream" gets closed */) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.decrypt(in, cpy);
      } catch (InterruptedIOException ignored) {
        // stop gracefully; interrupt indicates closure
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
    });
    decryptionThread.setUncaughtExceptionHandler((t, e) -> {
      fail.set(e);
    });
    decryptionThread.setName(decryptionThread.getName() + " [decryption]");
    decryptionThread.start();
  }

  @Override
  public void close() throws IOException {
    try {
      decryptionThread.interrupt();
      try {
        decryptionThread.join();
      } catch (InterruptedException ignored) {
      }
      Throwable exn = fail.get();
      if (exn != null) {
        throw new IOException("encryption failed", exn);
      }
    } finally {
      super.close();
    }
  }

}
