package cal.bkup;

import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

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

  public static String readPassword() {
    Console cons = System.console();
    if (cons == null) {
      throw new IllegalStateException("not connected to console");
    }
    char[] c1 = cons.readPassword("[%s]", "Password:");
    if (c1 == null) return null;
    char[] c2 = cons.readPassword("[%s]", "Confirm:");
    if (c2 == null) return null;
    if (!Arrays.equals(c1, c2)) {
      System.err.println("passwords do not match");
      return null;
    }
    return new String(c1);
  }

  public static long divideAndRoundUp(long numerator, long denominator) {
    return (numerator - 1) / denominator + 1;
  }

}
