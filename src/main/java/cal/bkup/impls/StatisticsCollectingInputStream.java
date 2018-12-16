package cal.bkup.impls;

import cal.bkup.Util;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

public class StatisticsCollectingInputStream extends FilterInputStream {

  private final MessageDigest digest;
  private long bytesRead;
  private boolean closed;

  public StatisticsCollectingInputStream(InputStream in) {
    super(in);
    digest = Util.sha256Digest();
    bytesRead = 0L;
    closed = false;
  }

  @Override
  public int read() throws IOException {
    int res = super.read();
    if (res >= 0) {
      digest.update((byte)res);
      bytesRead++;
    }
    return res;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int nread = super.read(b, off, len);
    if (nread > 0) {
      digest.update(b, off, nread);
      bytesRead += nread;
    }
    return nread;
  }

  @Override
  public long skip(long n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }

  public byte[] getSha256Digest() {
    return digest.digest();
  }

  public long getBytesRead() {
    return bytesRead;
  }

  public boolean isClosed() {
    return closed;
  }

  @Override
  public void close() throws IOException {
    super.close();
    closed = true;
  }
}
