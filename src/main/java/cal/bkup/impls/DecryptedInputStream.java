package cal.bkup.impls;

import cal.bkup.Util;
import es.vocali.util.AESCrypt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

public class DecryptedInputStream extends InputStream {

  private final InputStream stream;

  public DecryptedInputStream(InputStream wrappedStream, String password) throws IOException, GeneralSecurityException {
    // TODO: streaming???

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Util.copyStream(wrappedStream, os);
    byte[] bytes = os.toByteArray();

    AESCrypt crypt = new AESCrypt(password);
    os = new ByteArrayOutputStream();
    crypt.decrypt(bytes.length, new ByteArrayInputStream(bytes), os);

    stream = new ByteArrayInputStream(os.toByteArray());
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
    stream.close();
  }

}
