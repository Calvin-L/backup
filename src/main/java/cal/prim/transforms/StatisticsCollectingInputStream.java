package cal.prim.transforms;

import cal.bkup.Util;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.function.Consumer;

public class StatisticsCollectingInputStream extends FilterInputStream {

  private final MessageDigest digest;
  private final Consumer<@MustCall({}) StatisticsCollectingInputStream> onProgress;
  private long bytesRead;
  private boolean closed;

  public @MustCallAlias StatisticsCollectingInputStream(@MustCallAlias InputStream in, Consumer<@MustCall({}) StatisticsCollectingInputStream> onProgress) {
    super(in);
    digest = Util.sha256Digest();
    this.onProgress = onProgress;
    bytesRead = 0L;
    closed = false;
  }

  @Override
  public int read() throws IOException {
    int res = super.read();
    if (res >= 0) {
      digest.update((byte)res);
      bytesRead++;
      onProgress.accept(this);
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
      onProgress.accept(this);
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
    try {
      // .digest() resets the darn thing!
      // So, we gotta clone it first...
      return ((MessageDigest)digest.clone()).digest();
    } catch (CloneNotSupportedException e) {
      throw new UnsupportedOperationException(e);
    }
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
