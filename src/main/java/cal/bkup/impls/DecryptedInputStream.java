package cal.bkup.impls;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.GeneralSecurityException;
import java.util.concurrent.atomic.AtomicBoolean;

public class DecryptedInputStream extends InputStream {

  private final PipedInputStream stream;
  private final Thread decryptionThread;
  private final AtomicBoolean fail = new AtomicBoolean(false);

  public DecryptedInputStream(InputStream wrappedStream, String password) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Util.copyStream(wrappedStream, os);
    byte[] bytes = os.toByteArray();

    stream = new PipedInputStream(4096);
    PipedOutputStream out = new PipedOutputStream(stream);
    decryptionThread = new Thread(() -> {
      try (PipedOutputStream cpy = out; /* ensure that "out" gets closed */
           InputStream in = new ByteArrayInputStream(bytes)) {
        AESCrypt crypt = new AESCrypt(password);
        crypt.decrypt(bytes.length, in, cpy);
      } catch (InterruptedIOException ignored) {
        // stop gracefully; interrupt indicates closure
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException(e);
      }
    });
    decryptionThread.setUncaughtExceptionHandler((t, e) -> {
      e.printStackTrace();
      fail.set(true);
    });
    decryptionThread.setName(decryptionThread.getName() + " [encryption]");
    decryptionThread.setDaemon(true);
    decryptionThread.start();
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
    decryptionThread.interrupt();
    try {
      decryptionThread.join();
    } catch (InterruptedException ignored) {
    }
    if (fail.get()) {
      throw new IOException("decryption failed");
    }
    stream.close();
  }

}
