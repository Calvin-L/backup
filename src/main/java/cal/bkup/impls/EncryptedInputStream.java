package cal.bkup.impls;

import es.vocali.util.AESCrypt;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;

public class EncryptedInputStream extends InputStream {

  public static final int AES_VERSION = 2;
  private final PipedInputStream stream;
  private final Thread encryptionThread;
  private final AtomicBoolean fail = new AtomicBoolean(false);

  public EncryptedInputStream(InputStream wrappedStream, String password) throws GeneralSecurityException, IOException {
    stream = new PipedInputStream(4096);
    PipedOutputStream out = new PipedOutputStream(stream);
    encryptionThread = new Thread(() -> {
      try (PipedOutputStream cpy = out; /* ensure that "out" gets closed */
           InputStream in = new PoliteInputStream(wrappedStream) /* ensure that "wrappedStream" gets closed */) {
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
  public int read() throws IOException {
    return stream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return stream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return stream.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return stream.skip(n);
  }

  @Override
  public int available() throws IOException {
    return stream.available();
  }

  @Override
  public synchronized void mark(int readlimit) {
    stream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    stream.reset();
  }

  @Override
  public boolean markSupported() {
    return stream.markSupported();
  }

  @Override
  public void close() throws IOException {
    encryptionThread.interrupt();
    try {
      encryptionThread.join();
    } catch (InterruptedException ignored) {
    }
    if (fail.get()) {
      throw new IOException("encryption failed");
    }
    stream.close();
  }
}
