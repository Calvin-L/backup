package cal.bkup.impls;

import es.vocali.util.AESCrypt;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicReference;

import static cal.bkup.impls.EncryptedInputStream.AES_VERSION;

public class EncryptedOutputStream extends FilterOutputStream {

  private final Thread encryptionThread;
  private final AtomicReference<Throwable> thrownException = new AtomicReference<>(null);

  public EncryptedOutputStream(OutputStream wrappedStream, String password) throws IOException {
    super(new PipedOutputStream());
    PipedInputStream in = new PipedInputStream(4096);
    ((PipedOutputStream)out).connect(in);
    encryptionThread = new Thread(() -> {
      // The AESCrypt class does not respect the number returned by InputStream.read(byte[], ...).
      // So, we wrap the stream in a "polite" variant that does what it expects. This also ensures
      // that "in" and "wrappedStream" get closed.
      try (InputStream cpy = new PoliteInputStream(in);
           OutputStream out = wrappedStream) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.encrypt(AES_VERSION, cpy, out);
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
    });
    encryptionThread.setUncaughtExceptionHandler((t, e) -> thrownException.set(e));
    encryptionThread.setName(encryptionThread.getName() + " [encryption]");
    encryptionThread.start();
  }

  @Override
  public void close() throws IOException {
    super.close();
    try {
      encryptionThread.join();
    } catch (InterruptedException ignored) {
    }
    Throwable exn = thrownException.get();
    if (exn != null) {
      throw new IOException("encryption failed", exn);
    }
  }

}
