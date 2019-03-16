package cal.bkup;

import cal.bkup.types.IOConsumer;
import cal.bkup.types.Price;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Util {

  public static final long ONE_BYTE = 1;
  public static final long ONE_KB = ONE_BYTE * 1024;
  public static final long ONE_MB = ONE_KB * 1024;
  public static final long ONE_GB = ONE_MB * 1024;
  public static final long ONE_TB = ONE_GB * 1024;

  public static final int SUGGESTED_BUFFER_SIZE = 8192;

  private static final ThreadLocal<byte[]> MEM_BUFFER = ThreadLocal.withInitial(() -> new byte[SUGGESTED_BUFFER_SIZE]);

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

  public static MessageDigest sha256Digest() {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // This should never happen; all JREs are required to support
      // SHA-256 (as well as MD5 and SHA-1).
      throw new UnsupportedOperationException();
    }
    return md;
  }

  public static long copyStreamAndCaptureSize(InputStream in, OutputStream out) throws IOException {
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
    char[] c1 = cons.readPassword("%s", "Password for encryption: ");
    if (c1 == null) return null;
    char[] c2 = cons.readPassword("%s", "Confirm: ");
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
    MessageDigest md = sha256Digest();
    byte[] buf = MEM_BUFFER.get();
    int nread;
    while ((nread = in.read(buf)) >= 0) {
      md.update(buf, 0, nread);
    }
    return md.digest();
  }

  private static final String HEX_CHARS = "0123456789abcdef";
  public static String sha256toString(byte[] sha256) {
    StringBuilder builder = new StringBuilder();
    for (byte b : sha256) {
      int i = Byte.toUnsignedInt(b);
      builder.append(HEX_CHARS.charAt((i >> 4) & 0xF));
      builder.append(HEX_CHARS.charAt(i & 0xF));
    }
    return builder.toString();
  }

  /**
   * Convert a single hexadecimal digit to its integer value.
   * @param c a character
   * @return an int in the range [0, 15]
   */
  private static int hexValue(char c) {
    try {
      return Integer.parseInt("" + c, 16);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("character " + c + " is not a hex digit", e);
    }
  }

  /**
   * Inverse of {@link #sha256toString(byte[])}.
   * @param sha256
   * @return
   */
  public static byte[] stringToSha256(CharSequence sha256) {
    int len = sha256.length();
    if (len != 64) {
      throw new IllegalArgumentException("string has the wrong length to be a SHA-256 sum (should be 64, was " + len + ')');
    }
    byte[] sum = new byte[32];
    for (int i = 0; i < sum.length; ++i) {
      char c1 = sha256.charAt(i * 2);
      char c2 = sha256.charAt(i * 2 + 1);
      int val1 = hexValue(c1);
      int val2 = hexValue(c2);
      sum[i] = (byte)(val1 << 4 | val2);
    }
    return sum;
  }

  public static InputStream createInputStream(IOConsumer<OutputStream> writer) throws IOException {
    PipedInputStream in = new PipedInputStream(SUGGESTED_BUFFER_SIZE);
    PipedOutputStream out = new PipedOutputStream(in);
    AtomicReference<Exception> err = new AtomicReference<>(null);
    Thread t = new Thread(() -> {
      try {
        writer.accept(out);
      } catch (Exception e) {
        err.set(e);
      }
    });
    t.start();

    return new FilterInputStream(in) {
      @Override
      public void close() throws IOException {
        try {
          t.interrupt();
          t.join();
          Exception e = err.get();
          if (e != null) {
            throw new IOException("byte producer failed", e);
          }
        } catch (InterruptedException e) {
          throw new InterruptedIOException();
        } finally {
          super.close();
        }
      }
    };
  }

  public static long maximum(long[] array) throws NoSuchElementException {
    if (array.length == 0) {
      throw new NoSuchElementException();
    }
    long result = array[0];
    for (int i = 1; i < array.length; ++i) {
      result = Math.max(result, array[i]);
    }
    return result;
  }

  public static void run(String... args) throws IOException {
    Process p = new ProcessBuilder(args)
            .inheritIO()
            .start();
    while (true) {
      try {
        p.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
  }

}
