package cal.bkup;

import cal.bkup.types.Price;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public abstract class Util {

  public static final long ONE_BYTE = 1;
  public static final long ONE_KB = ONE_BYTE * 1024;
  public static final long ONE_MB = ONE_KB * 1024;
  public static final long ONE_GB = ONE_MB * 1024;
  public static final long ONE_TB = ONE_GB * 1024;

  private static final ThreadLocal<byte[]> MEM_BUFFER = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[4096];
    }
  };

  public static long copyStream(InputStream in, OutputStream out) throws IOException {
    byte[] buf = MEM_BUFFER.get();
    long count = 0;
    int n;
    while ((n = in.read(buf)) >= 0) {
      out.write(buf, 0, n);
      count += n;
    }
    return count;
  }

  public static byte[] read(InputStream in) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      copyStream(in, out);
      return out.toByteArray();
    }
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
    return (numerator + denominator - 1) / denominator;
  }

  public static String formatSize(long l) {
    if (l > ONE_TB) return divideAndRoundUp(l, ONE_TB) + " Tb";
    if (l > ONE_GB) return divideAndRoundUp(l, ONE_GB) + " Gb";
    if (l > ONE_MB) return divideAndRoundUp(l, ONE_MB) + " Mb";
    if (l > ONE_KB) return divideAndRoundUp(l, ONE_KB) + " Kb";
    return l + " bytes";
  }

  public static String formatPrice(Price p) {
    long pennies = p.valueInCents().longValue();
    boolean pos = true;
    if (pennies < 0) {
      pos = false;
      pennies = -pennies;
    }
    return (pos ? "" : "-") + "$" + (pennies / 100) + '.' + (pennies % 100 / 10) + (pennies % 100 % 10);
  }

  public static <T extends Comparable<T>> boolean lt(T x, T y) {
    return x.compareTo(y) < 0;
  }

  public static <T extends Comparable<T>> boolean le(T x, T y) {
    return x.compareTo(y) <= 0;
  }

  public static <T extends Comparable<T>> boolean gt(T x, T y) {
    return x.compareTo(y) > 0;
  }

  public static <T extends Comparable<T>> boolean ge(T x, T y) {
    return x.compareTo(y) >= 0;
  }

  public static byte[] sha256(InputStream in) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // This should never happen; all JREs are required to support
      // SHA-256 (as well as MD5 and SHA-1).
      throw new UnsupportedOperationException();
    }
    byte[] buf = MEM_BUFFER.get();
    int nread;
    while ((nread = in.read(buf)) >= 0) {
      md.update(buf, 0, nread);
    }
    return md.digest();
  }

}
