package cal.bkup.impls;

import es.vocali.util.AESCrypt;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;

import static cal.bkup.impls.EncryptedInputStream.AES_VERSION;

public class EncryptedOutputStream extends OutputStream {

  private final OutputStream stream;
  private final Thread encryptionThread;
  private final AtomicBoolean fail = new AtomicBoolean(false);

  public EncryptedOutputStream(OutputStream wrappedStream, String password) throws IOException {
    PipedInputStream in = new PipedInputStream(4096);
    stream = new PipedOutputStream(in);
    encryptionThread = new Thread(() -> {
      try (InputStream cpy = in; /* ensure that "in" gets closed */
           OutputStream out = wrappedStream /* ensure that "wrappedStream" gets closed */) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.encrypt(AES_VERSION, cpy, out);
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
    });
    encryptionThread.setUncaughtExceptionHandler((t, e) -> {
      e.printStackTrace();
      fail.set(true);
    });
    encryptionThread.setName(encryptionThread.getName() + " [encryption]");
    encryptionThread.setDaemon(true);
    encryptionThread.start();
  }

  @Override
  public void write(byte[] b) throws IOException {
    stream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    stream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    stream.flush();
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
  }

  @Override
  public void close() throws IOException {
    stream.close();
    try {
      encryptionThread.join();
    } catch (InterruptedException ignored) {
    }
    if (fail.get()) {
      throw new IOException("encryption failed");
    }
  }

}
