package cal.bkup;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

  static <R> R fail() {
    throw new RuntimeException("failure");
  }

}
