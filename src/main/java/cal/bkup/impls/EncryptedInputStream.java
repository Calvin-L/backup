package cal.bkup.impls;

import es.vocali.util.AESCrypt;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;

public class EncryptedInputStream extends FilterInputStream {

  public static final int AES_VERSION = 2;
  private final Thread encryptionThread;
  private final AtomicBoolean fail = new AtomicBoolean(false);

  public EncryptedInputStream(InputStream wrappedStream, String password) throws GeneralSecurityException, IOException {
    super(new PipedInputStream(4096));
    PipedOutputStream out = new PipedOutputStream((PipedInputStream)in);
    encryptionThread = new Thread(() -> {
      try (PipedOutputStream cpy = out; /* ensure that "out" gets closed */
           InputStream in = wrappedStream /* ensure that "wrappedStream" gets closed */) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.encrypt(AES_VERSION, in, cpy);
      } catch (InterruptedIOException ignored) {
        // stop gracefully; interrupt indicates closure
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
    });
    encryptionThread.setUncaughtExceptionHandler((t, e) -> {
      e.printStackTrace();
      fail.set(true);
    });
    encryptionThread.setName(encryptionThread.getName() + " [encryption]");
    encryptionThread.start();
  }

  @Override
  public void close() throws IOException {
    try {
      encryptionThread.interrupt();
      try {
        encryptionThread.join();
      } catch (InterruptedException ignored) {
      }
      if (fail.get()) {
        throw new IOException("encryption failed");
      }
    } finally {
      super.close();
    }
  }
}
