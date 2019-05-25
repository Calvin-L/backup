package cal.bkup.impls;

import cal.bkup.types.BackupReport;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SystemId;
import cal.prim.fs.HardLink;
import cal.prim.fs.SymLink;
import cal.prim.storage.ConsistentBlob;
import com.google.common.collect.ImmutableSet;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A <code>BackupIndex</code> is a data structure that stores metadata about backed-up
 * files.  It is <em>transient</em>&mdash;the data stored in the index may not have been
 * permanently stored anywhere.  However, clients should ensure that any files referred
 * to in the index <em>have</em> been backed up as described.
 *
 * <p>An index stores information for several systems.  For each system, it stores details
 * for each known file {@link Path} on that system.  For each <code>Path</code>, it
 * stores a list of {@link Revision Revisions}.
 *
 * <p>To store an index durably, combine {@link ConsistentBlob} and
 * {@link cal.bkup.types.IndexFormat}.
 *
 * <p>The <code>BackupIndex</code> object does not actually back up files.  Any client code
 * that uses a <code>BackupIndex</code> must ensure that the index's view of the world is
 * honest: if the index reports that a file is backed up to a permanent location, then it
 * really has been.
 *
 * <p>Instances of this class are completely thread-safe.  All modification methods are
 * atomic.  All query methods return immutable copies of their results, so their
 * answers will not be affected by the actions of other threads in the future.
 */
@EqualsAndHashCode
public class BackupIndex {

  public enum FileType {
    REGULAR_FILE,
    HARD_LINK,
    SOFT_LINK,
    TOMBSTONE
  }

  @ToString
  @EqualsAndHashCode
  public static class Revision {
    public final FileType type;

    // type is REGULAR_FILE
    /**
     * The file's modification time, truncated to millisecond precision.
     */
    public final Instant modTime;
    public final Sha256AndSize summary;

    // type is SOFT_LINK or HARD_LINK
    public final Path linkTarget;

    private Revision() {
      this.type = FileType.TOMBSTONE;
      this.modTime = null;
      this.summary = null;
      this.linkTarget = null;
    }

    private Revision(Instant modTime, Sha256AndSize summary) {
      this.type = FileType.REGULAR_FILE;
      this.modTime = modTime;
      this.summary = summary;
      this.linkTarget = null;
    }

    public Revision(SymLink link) {
      this.type = FileType.SOFT_LINK;
      this.modTime = null;
      this.summary = null;
      this.linkTarget = link.getDestination();
    }

    public Revision(HardLink link) {
      this.type = FileType.HARD_LINK;
      this.modTime = null;
      this.summary = null;
      this.linkTarget = link.getDestination();
    }
  }

  private final Map<SystemId, Map<Path, List<Revision>>> files;
  private final Map<Sha256AndSize, BackupReport> blobs;

  public BackupIndex() {
    this(new HashMap<>(), new HashMap<>());
  }

  private BackupIndex(Map<SystemId, Map<Path, List<Revision>>> data, Map<Sha256AndSize, BackupReport> blobs) {
    this.files = data;
    this.blobs = blobs;
  }

  public synchronized @Nullable BackupReport lookupBlob(Sha256AndSize content) {
    return blobs.get(content);
  }

  public synchronized void forgetBlob(Sha256AndSize content) {
    blobs.remove(content);
  }

  public synchronized Stream<Sha256AndSize> listBlobs() {
    return new ArrayList<>(blobs.keySet()).stream();
  }

  public synchronized void addBackedUpBlob(Sha256AndSize content, BackupReport backupReport) {
    BackupReport previous = blobs.putIfAbsent(content, backupReport);
    if (previous != null) {
      throw new IllegalArgumentException("the blob " + content + " is already known");
    }
  }

  public synchronized Set<SystemId> knownSystems() {
    return ImmutableSet.copyOf(files.keySet());
  }

  public synchronized Set<Path> knownPaths(SystemId system) {
    Map<Path, List<Revision>> info = files.get(system);
    return info != null ? ImmutableSet.copyOf(info.keySet()) : Collections.emptySet();
  }

  public synchronized List<Revision> getInfo(SystemId system, Path path) {
    Map<Path, List<Revision>> info = files.get(system);
    if (info == null) {
      return Collections.emptyList();
    }
    List<Revision> revisions = info.get(path);
    return revisions != null ? new ArrayList<>(revisions) : Collections.emptyList();
  }

  public synchronized @Nullable Revision mostRecentRevision(SystemId system, Path path) {
    List<Revision> revisions = getInfo(system, path);
    if (revisions.isEmpty()) {
      return null;
    }
    return revisions.get(revisions.size() - 1);
  }

  private List<Revision> findOrAddRevisionList(SystemId system, Path path) {
    return files.computeIfAbsent(system, s -> new HashMap<>())
            .computeIfAbsent(path, p -> new ArrayList<>());
  }

  /**
   * Add a regular-file type revision for a path.
   *
   * <p>NOTE: <code>modTime</code> will be truncated to millisecond precision.
   *
   * @param system the system that the file is on
   * @param path the path to the file
   * @param modTime the file's modification time
   * @param contentSummary a summary of the file's contents on disk
   */
  public synchronized void appendRevision(SystemId system, Path path, Instant modTime, Sha256AndSize contentSummary) {
    if (lookupBlob(contentSummary) == null) {
      throw new IllegalArgumentException("Refusing to add backed up file that references nonexistent blob " + contentSummary);
    }
    modTime = modTime.truncatedTo(ChronoUnit.MILLIS);
    findOrAddRevisionList(system, path).add(new Revision(modTime, contentSummary));
  }

  public synchronized void appendRevision(SystemId system, Path path, HardLink link) {
    findOrAddRevisionList(system, path).add(new Revision(link));
  }

  public synchronized void appendRevision(SystemId system, Path path, SymLink link) {
    findOrAddRevisionList(system, path).add(new Revision(link));
  }

  public synchronized void appendTombstone(SystemId system, Path path) {
    findOrAddRevisionList(system, path).add(new Revision());
  }

  public static class MergeConflict extends Exception {
    public MergeConflict(String message) {
      super(message);
    }
  }

  @FunctionalInterface
  private interface Merger<T> {
    T merge(T x, T y) throws MergeConflict;
  }

  private static final class MergeRequiringEquality<T> implements Merger<T> {
    @Override
    public T merge(T x, T y) throws MergeConflict {
      if (Objects.equals(x, y)) {
        return x;
      }
      throw new MergeConflict("not equal: " + x + " and " + y);
    }
  }

  private static final class MergeWithArbitraryChoice<T> implements Merger<T> {
    @Override
    public T merge(T x, T y) {
      return x;
    }
  }

  private static final class MergeMaps<K, V> implements Merger<Map<K, V>> {
    private final Merger<V> valueMerger;

    public MergeMaps(Merger<V> valueMerger) {
      this.valueMerger = valueMerger;
    }

    @Override
    public Map<K, V> merge(Map<K, V> x, Map<K, V> y) throws MergeConflict {
      Map<K, V> result = new HashMap<>(x);
      for (Map.Entry<K, V> entry : y.entrySet()) {
        K key = entry.getKey();
        V val = entry.getValue();
        if (result.containsKey(key)) {
          result.put(key, valueMerger.merge(result.get(key), val));
        } else {
          result.put(key, val);
        }
      }
      return result;
    }
  }

  private static final class MergeLists<T> implements Merger<List<T>> {
    private final Merger<T> valueMerger;

    public MergeLists(Merger<T> valueMerger) {
      this.valueMerger = valueMerger;
    }

    @Override
    public List<T> merge(List<T> x, List<T> y) throws MergeConflict {
      final List<T> longerList;
      final List<T> shorterList;
      if (x.size() <= y.size()) {
        shorterList = x;
        longerList = y;
      } else {
        shorterList = y;
        longerList = x;
      }
      final List<T> result = new ArrayList<>(longerList.size());
      for (int i = 0; i < shorterList.size(); ++i) {
        result.add(valueMerger.merge(shorterList.get(i), longerList.get(i)));
      }
      for (int i = shorterList.size(); i < longerList.size(); ++i) {
        result.add(longerList.get(i));
      }
      return result;
    }
  }

  private static final Merger<Map<SystemId, Map<Path, List<Revision>>>> FILE_MERGER = new MergeMaps<>(new MergeMaps<>(new MergeLists<>(new MergeRequiringEquality<>())));
  private static final Merger<Map<Sha256AndSize, BackupReport>> BLOB_MERGER = new MergeMaps<>(new MergeWithArbitraryChoice<>());

  public BackupIndex merge(BackupIndex other) throws MergeConflict {
    final Map<SystemId, Map<Path, List<Revision>>> files = FILE_MERGER.merge(this.files, other.files);
    final Map<Sha256AndSize, BackupReport> blobs = BLOB_MERGER.merge(this.blobs, other.blobs);
    return new BackupIndex(files, blobs);
  }

}
