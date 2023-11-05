package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.impls.ProgressDisplay.Task;
import cal.bkup.types.Sha256AndSize;
import cal.prim.Pair;
import cal.prim.fs.Filesystem;
import cal.prim.fs.RegularFile;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.Encryption;
import cal.prim.transforms.StatisticsCollectingInputStream;
import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.NotOwning;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

class BatchFileInputStream extends InputStream {
  private final Collection<Pair<RegularFile, Sha256AndSize>> files;
  private final ProgressDisplay display;
  private final Map<Path, Long> offsetsAtTarget;
  private final Map<Path, Long> sizesAtTarget;
  private final Map<Path, Sha256AndSize> actualSummaries;
  private final Task batchTask;
  private final Filesystem fs;
  private final String key;
  private final BlobTransformer transformer;
  private final Iterator<Pair<RegularFile, Sha256AndSize>> remaining;
  private long bytesSent;
  private int finishedFiles;
  private @Owning @Nullable CurrentFileState current;

  private record CurrentFileState(
    RegularFile currentFile,
    long startOffsetOfCurrentFile,
    @NotOwning StatisticsCollectingInputStream stats,
    @Owning InputStream current,
    Task task) implements Closeable {

    private CurrentFileState(RegularFile currentFile, long startOffsetOfCurrentFile, @NotOwning StatisticsCollectingInputStream stats, @Owning InputStream current, Task task) {
      this.currentFile = currentFile;
      this.startOffsetOfCurrentFile = startOffsetOfCurrentFile;
      this.stats = stats;
      this.current = current;
      this.task = task;
    }

    @EnsuresCalledMethods(value = "current", methods = {"close"})
    public void close() throws IOException {
      current.close();
    }
  }

  public BatchFileInputStream(Collection<Pair<RegularFile, Sha256AndSize>> files, ProgressDisplay display, Map<Path, Long> offsetsAtTarget, Map<Path, Long> sizesAtTarget, Map<Path, Sha256AndSize> actualSummaries, Task batchTask, Filesystem fs, String key, BlobTransformer transformer) throws IOException {
    this.files = files;
    this.display = display;
    this.offsetsAtTarget = offsetsAtTarget;
    this.sizesAtTarget = sizesAtTarget;
    this.actualSummaries = actualSummaries;
    this.batchTask = batchTask;
    this.fs = fs;
    this.key = key;
    this.transformer = transformer;
    this.remaining = files.iterator();
    this.bytesSent = 0L;
    this.finishedFiles = 0;

    if (this.current != null) {
      this.current.close();
      throw new IllegalStateException("Somehow `current` got initialized early");
    }
    this.current = null;
  }

  @SuppressWarnings("required.method.not.called") // TODO: false positive?
  private void closeCurrent() throws IOException {
    if (current != null) {
      var toClose = current;
      ++finishedFiles;
      toClose.close();
      assert toClose.stats.isClosed();
      var path = toClose.currentFile.path();
      offsetsAtTarget.put(path, toClose.startOffsetOfCurrentFile);
      sizesAtTarget.put(path, bytesSent - toClose.startOffsetOfCurrentFile);
      actualSummaries.put(path, new Sha256AndSize(
          toClose.stats.getSha256Digest(),
          toClose.stats.getBytesRead()));
      display.finishTask(toClose.task);
      current = null;
    }
  }

  @SuppressWarnings("missing.creates.mustcall.for")
  private void openNext() throws IOException {
    display.reportProgress(batchTask, finishedFiles, files.size());
    var currentFile = remaining.next().fst();
    var task = display.startTask("Upload " + currentFile.path());
    var startOffsetOfCurrentFile = bytesSent;
    var rawStream = fs.openRegularFileForReading(currentFile.path());
    try {
      var newStats = new StatisticsCollectingInputStream(Util.buffered(rawStream), s ->
          display.reportProgress(task, s.getBytesRead(), currentFile.sizeInBytes()));
      var stream = transformer.followedBy(new Encryption(key)).apply(newStats);
      if (current != null) {
        current.close();
        throw new IllegalStateException("openNext() called before closeCurrent()");
      }
      current = new CurrentFileState(
          currentFile,
          startOffsetOfCurrentFile,
          newStats,
          stream,
          task);
    } catch (Exception e) {
      try {
        rawStream.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
  }

  @Override
  public int read() throws IOException {
    byte[] a = new byte[1];
    int count = read(a, 0, 1);
    return count > 0 ? Byte.toUnsignedInt(a[0]) : -1;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    while (current != null || remaining.hasNext()) {
      if (current == null) {
        openNext();
      }

      if (current != null) {
        int nread = current.current.read(b, off, len);

        if (nread >= 0) {
          bytesSent += nread;
          return nread;
        }

        closeCurrent();
      }
    }
    return -1;
  }

  @EnsuresCalledMethods(value = "current", methods = "close")
  @Override
  public void close() throws IOException {
    if (current != null) {
      current.close();
    }

    if (current != null || remaining.hasNext()) {
      throw new RuntimeException("not all of the stream was consumed");
    }
  }
}
