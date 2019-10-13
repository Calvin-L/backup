package cal.bkup;

import cal.bkup.types.Sha256AndSize;
import cal.prim.IOConsumer;
import cal.prim.QuietAutoCloseable;
import cal.prim.transforms.StatisticsCollectingInputStream;

import java.io.BufferedInputStream;
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
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public abstract class Util {

  public static final long ONE_BYTE = 1;
  public static final long ONE_KB = ONE_BYTE * 1024;
  public static final long ONE_MB = ONE_KB * 1024;
  public static final long ONE_GB = ONE_MB * 1024;
  public static final long ONE_TB = ONE_GB * 1024;

  /**
   * The suggested size of in-memory byte buffers for I/O.
   * The value is 8192, which is currently the size used by {@link BufferedInputStream}
   * on desktop JVMs.
   *
   * <p>Performance note: there is a large benefit to having every layer of a software
   * system use the same buffer size.  If data from one stream using one buffer size is
   * piped to a consumer reading with a different buffer size, the mismatch can cause
   * an unexpected performance hit.
   */
  public static final int SUGGESTED_BUFFER_SIZE = 8192;

  /**
   * A thread-local byte array of {@link #SUGGESTED_BUFFER_SIZE} bytes.
   */
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

  public static Sha256AndSize summarize(InputStream in, Consumer<StatisticsCollectingInputStream> progressCallback) throws IOException {
    StatisticsCollectingInputStream s = new StatisticsCollectingInputStream(in, progressCallback);
    drain(s);
    return new Sha256AndSize(s.getSha256Digest(), s.getBytesRead());
  }

  public static BufferedInputStream buffered(InputStream in) {
    return new BufferedInputStream(in, SUGGESTED_BUFFER_SIZE);
  }

  public static byte[] read(InputStream in) throws IOException {
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      copyStream(in, out);
      return out.toByteArray();
    }
  }

  public static String readPassword(String prompt) {
    Console cons = System.console();
    if (cons == null) {
      throw new IllegalStateException("not connected to console");
    }
    char[] c1 = cons.readPassword("%s: ", prompt);
    if (c1 == null) return null;
    char[] c2 = cons.readPassword("Confirm: ");
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

  public static InputStream createInputStream(IOConsumer<OutputStream> writer) {
    PipedInputStream in = new PipedInputStream(SUGGESTED_BUFFER_SIZE);
    CountDownLatch gate = new CountDownLatch(1);
    AtomicReference<Exception> err = new AtomicReference<>(null);

    Thread t = new Thread(() -> {
      try (PipedOutputStream out = new PipedOutputStream(in)) {
        gate.countDown();
        writer.accept(out);
      } catch (Exception e) {
        err.set(e);
      } finally {
        // If an exception was thrown constructing the PipedOutputStream,
        // then unblock the waiting parent thread.
        while (gate.getCount() > 0) {
          gate.countDown();
        }
      }
    });
    t.start();

    for (;;) {
      try {
        gate.await();
        break;
      } catch (InterruptedException ignored) {
      }
    }

    return new FilterInputStream(in) {
      @Override
      public void close() throws IOException {
        try {
//          t.interrupt();
          long dropped = Util.drain(this.in);
          if (dropped > 0) {
            System.err.println("Dropped " + dropped + " bytes");
          }
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

  public static long drain(InputStream in) throws IOException {
    byte[] buf = MEM_BUFFER.get();
    long count = 0;
    int n;
    while ((n = in.read(buf)) >= 0) {
      count += n;
    }
    return count;
  }

  public static int readChunk(InputStream in, byte[] chunk) throws IOException {
    int soFar = 0;
    int n;
    while (soFar < chunk.length && (n = in.read(chunk, soFar, chunk.length - soFar)) >= 0) {
      soFar += n;
    }
    return soFar;
  }

  public static Thread async(Runnable job) {
    Thread t = new Thread(job);
    t.start();
    return t;
  }

  /**
   * Prevent the current thread from stopping during a JVM shutdown.
   * One common source of shutdowns is the Unix <code>SIGINT</code> signal that is
   * sent when a console user presses Ctrl+C.
   *
   * <p>Note that this method does not prevent all possible causes of process
   * termination; for instance, the <code>SIGKILL</code> signal cannot be prevented.
   *
   * <p>Sample usage for this method:
   *
   * <pre>
   *   Runnable onShutdown = () -&gt; { System.err.println("Ignoring shutdown..."); };
   *   try (QuietAutoCloseable ignored = Util.catchShutdown(onShutdown)) {
   *     // work uninterrupted
   *   }
   *   // normal shutdowns are possible again out here
   * </pre>
   *
   * @param onShutdown a hook that is run when a shutdown would have occurred
   * @return an object whose {@link QuietAutoCloseable#close() close method} re-enables normal shutdown
   */
  public static QuietAutoCloseable catchShutdown(Runnable onShutdown) {
    // Source of this trick:
    // https://stackoverflow.com/a/2922031/784284

    final var unstoppableThread = Thread.currentThread();
    final var runtime = Runtime.getRuntime();
    final var shutdownHook = new Thread(() -> {
      onShutdown.run();
      for (;;) {
        try {
          unstoppableThread.join();
          return;
        } catch (InterruptedException ignored) {
          System.err.println("Ignoring interrupt...");
        }
      }
    });

    runtime.addShutdownHook(shutdownHook);
    return () -> {
      try {
        runtime.removeShutdownHook(shutdownHook);
      } catch (IllegalStateException ignored) {
        // This happens if the JVM is shutting down, in which case our hook
        // is already committed to running and removing it won't matter.
      }
    };
  }

  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private static final String PASSWORD_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  private static final byte PASSWORD_LEN = 30;
  public static synchronized String randomPassword() {
    char[] chars = new char[PASSWORD_LEN];
    for (int i = 0; i < PASSWORD_LEN; ++i) {
      chars[i] = PASSWORD_CHARS.charAt(SECURE_RANDOM.nextInt(PASSWORD_CHARS.length()));
    }
    return new String(chars);
  }

  public static <T extends Comparable<T>> List<T> sorted(Collection<T> elements) {
    var result = new ArrayList<>(elements);
    result.sort(Comparator.naturalOrder());
    return result;
  }

}
