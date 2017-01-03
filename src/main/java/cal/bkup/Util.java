package cal.bkup;

import cal.bkup.types.IOConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class Util {

  public static long copyStream(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[4096];
    long count = 0;
    int n;
    while ((n = in.read(buf)) >= 0) {
      out.write(buf, 0, n);
      count += n;
    }
    return count;
  }

  public static void ensure(boolean b) {
    if (!b) {
      throw new RuntimeException("condition failure");
    }
  }

  public static <R> R fail() {
    throw new RuntimeException("failure");
  }

  public static OutputStream makeWriter(IOConsumer<InputStream> c) throws IOException {
    PipedInputStream in = new PipedInputStream(4096);
    PipedOutputStream out = new PipedOutputStream(in);
    AtomicBoolean fail = new AtomicBoolean(false);

    Thread t = new Thread(() -> {
      try {
        c.accept(in);
      } catch (IOException e) {
        fail.set(true);
      }
    });
    t.start();

    return new OutputStream() {
      @Override
      public void write(byte[] b) throws IOException {
        out.write(b);
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
      }

      @Override
      public void flush() throws IOException {
        out.flush();
      }

      @Override
      public void write(int b) throws IOException {
        out.write(b);
      }

      @Override
      public void close() throws IOException {
        out.close();
        try {
          t.join();
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }
    };
  }

}
