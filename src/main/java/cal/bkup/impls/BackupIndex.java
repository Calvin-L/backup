package cal.bkup.impls;

import cal.bkup.types.BackupReport;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.fs.HardLink;
import cal.prim.fs.SymLink;
import cal.prim.storage.ConsistentBlob;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
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
public class BackupIndex {

  public sealed interface Revision permits RegularFileRev, SoftLinkRev, HardLinkRev, TombstoneRev {
    long backupNumber();
  }

  public record RegularFileRev(long backupNumber, Instant modTime, Sha256AndSize summary) implements Revision {
  }

  public record SoftLinkRev(long backupNumber, Path target) implements Revision {
  }

  public record HardLinkRev(long backupNumber, Path target) implements Revision {
  }

  public record TombstoneRev(long backupNumber) implements Revision {
  }

  public record BackupMetadata(Instant startTime, @Nullable Instant endTime, long backupNumber) {
  }

  private final Map<SystemId, List<BackupMetadata>> backupHistory;
  private final Map<SystemId, Map<Path, List<Revision>>> files;
  private final Map<Sha256AndSize, BackupReport> blobs;

  /**
   * The garbage collection (GC) generation is used to fence out backups when
   * cleanup happens.  Cleanups bump this number, preventing a {@link #merge(BackupIndex)}
   * with concurrent backups.
   *
   * @see BackerUpper#planCleanup(String, Duration, StorageCostModel)
   */
  private BigInteger cleanupGeneration;

  public BackupIndex() {
    this(new HashMap<>(), new HashMap<>(), new HashMap<>(), BigInteger.ZERO);
  }

  private BackupIndex(
          Map<SystemId, List<BackupMetadata>> backupHistory,
          Map<SystemId, Map<Path, List<Revision>>> data,
          Map<Sha256AndSize, BackupReport> blobs,
          BigInteger cleanupGeneration) {
    this.backupHistory = backupHistory;
    this.files = data;
    this.blobs = blobs;
    this.cleanupGeneration = cleanupGeneration;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BackupIndex that = (BackupIndex) o;
    return backupHistory.equals(that.backupHistory) && files.equals(that.files) && blobs.equals(that.blobs) && cleanupGeneration.equals(that.cleanupGeneration);
  }

  @Override
  public int hashCode() {
    return Objects.hash(backupHistory, files, blobs, cleanupGeneration);
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

  public synchronized BackupReport addOrCanonicalizeBackedUpBlob(Sha256AndSize content, BackupReport backupReport) {
    BackupReport previous = blobs.putIfAbsent(content, backupReport);
    return previous != null ? previous : backupReport;
  }

  public synchronized Set<SystemId> knownSystems() {
    return ImmutableSet.copyOf(files.keySet());
  }

  public synchronized BackupMetadata findBackup(SystemId system, long backupNumber) {
    for (var backup : backupHistory.getOrDefault(system, Collections.emptyList())) {
      if (backup.backupNumber() == backupNumber) {
        return backup;
      }
    }
    throw new NoSuchElementException("No backup with number " + backupNumber + " on system " + system);
  }

  public synchronized List<BackupMetadata> knownBackups(SystemId system) {
    var backups = backupHistory.get(system);
    return backups != null ? ImmutableList.copyOf(backups) : Collections.emptyList();
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

  public synchronized Revision resolveHardLinkTarget(SystemId system, HardLinkRev hardLink) {
    return getInfo(system, hardLink.target()).stream()
            .filter(possibleTarget -> possibleTarget.backupNumber() <= hardLink.backupNumber())
            .max(Comparator.comparingLong(Revision::backupNumber))
            .orElseThrow(() -> new IllegalStateException("Hard link " + hardLink + " has no target"));
  }

  private <T> T lastEntry(List<T> list) {
    return list.get(list.size() - 1);
  }

  public synchronized @Nullable Revision mostRecentRevision(SystemId system, Path path) {
    List<Revision> revisions = getInfo(system, path);
    if (revisions.isEmpty()) {
      return null;
    }
    return lastEntry(revisions);
  }

  public synchronized void forgetRevision(SystemId system, Path path, Revision revision) {
    if (revision.equals(mostRecentRevision(system, path)) && !(revision instanceof TombstoneRev)) {
      throw new IllegalArgumentException("Can't forget most recent revision!");
    }
    Map<Path, List<Revision>> info = files.get(system);
    if (info != null) {
      List<Revision> revs = info.get(path);
      if (revs != null) {
        revs.remove(revision);
        if (revs.isEmpty()) {
          info.remove(path);
        }
      }
      if (info.isEmpty()) {
        files.remove(system);
      }
    }
  }

  public synchronized void addBackupInfo(SystemId system, BackupMetadata info) {
    backupHistory.computeIfAbsent(system, s -> new ArrayList<>()).add(info);
  }

  public synchronized void forgetBackup(SystemId system, BackupMetadata info) {
    for (Path p : knownPaths(system)) {
      for (Revision r : getInfo(system, p)) {
        if (r.backupNumber() == info.backupNumber) {
          throw new IllegalArgumentException("Can't forget backup while a revision exists");
        }
      }
    }
    Optional.ofNullable(backupHistory.get(system)).ifPresent(backups -> backups.remove(info));
  }

  public synchronized void forgetSystem(SystemId system) {
    var backups = backupHistory.get(system);
    if (backups != null && !backups.isEmpty()) {
      throw new IllegalArgumentException("Can't forget system with known backups");
    }

    var paths = files.get(system);
    if (paths != null && !paths.isEmpty()) {
      throw new IllegalStateException("There are known paths for a system with no backups...?");
    }

    backupHistory.remove(system);
    files.remove(system);
  }

  public synchronized BackupMetadata startBackup(SystemId system, Instant now) {
    now = now.truncatedTo(ChronoUnit.MILLIS);
    List<BackupMetadata> history = backupHistory.computeIfAbsent(system, s -> new ArrayList<>());
    BackupMetadata result =
            history.isEmpty()
                    ? new BackupMetadata(now, null, 0)
                    : new BackupMetadata(now, null, lastEntry(history).backupNumber() + 1);
    history.add(result);
    return result;
  }

  public synchronized void finishBackup(SystemId system, BackupMetadata info, Instant now) {
    now = now.truncatedTo(ChronoUnit.MILLIS);
    List<BackupMetadata> history = backupHistory.get(system);
    if (history == null) {
      throw new IllegalArgumentException("No backup history for system " + system);
    }
    for (int i = 0; i < history.size(); ++i) {
      BackupMetadata old = history.get(i);
      if (old.backupNumber() == info.backupNumber()) {
        if (old.endTime() != null) {
          throw new IllegalStateException("The backup " + info + " was already finished");
        }
        history.set(i, new BackupMetadata(
                old.startTime(),
                now,
                old.backupNumber()));
        return;
      }
    }
    throw new IllegalArgumentException("The backup metadata " + info + " is not known");
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
  public synchronized void appendRevision(SystemId system, BackupMetadata backup, Path path, Instant modTime, Sha256AndSize contentSummary) {
    if (lookupBlob(contentSummary) == null) {
      throw new IllegalArgumentException("Refusing to add backed up file that references nonexistent blob " + contentSummary);
    }
    modTime = modTime.truncatedTo(ChronoUnit.MILLIS);
    findOrAddRevisionList(system, path).add(new RegularFileRev(backup.backupNumber(), modTime, contentSummary));
  }

  public synchronized void appendRevision(SystemId system, BackupMetadata backup, Path path, HardLink link) {
    findOrAddRevisionList(system, path).add(new HardLinkRev(backup.backupNumber(), link.destination()));
  }

  public synchronized void appendRevision(SystemId system, BackupMetadata backup, Path path, SymLink link) {
    findOrAddRevisionList(system, path).add(new SoftLinkRev(backup.backupNumber(), link.destination()));
  }

  public synchronized void appendTombstone(SystemId system, BackupMetadata backup, Path path) {
    findOrAddRevisionList(system, path).add(new TombstoneRev(backup.backupNumber()));
  }

  public synchronized void setCleanupGeneration(BigInteger newCleanupGeneration) {
    assert newCleanupGeneration.compareTo(cleanupGeneration) >= 0;
    cleanupGeneration = newCleanupGeneration;
  }

  public synchronized void bumpCleanupGeneration() {
    setCleanupGeneration(cleanupGeneration.add(BigInteger.ONE));
  }

  public BigInteger getCleanupGeneration() {
    return cleanupGeneration;
  }

  public void checkIntegrity() {
    List<String> errors = new ArrayList<>();
    for (var sys : knownSystems()) {
      for (var p : knownPaths(sys)) {
        for (var rev : getInfo(sys, p)) {
          // NOTE: can use pattern-matching switch for this in the future
          if (rev instanceof RegularFileRev f) {
            if (lookupBlob(f.summary()) == null) {
              errors.add(sys + ":" + p + "@" + rev + " contents missing (" + f.summary() + ")");
            }
          } else if (rev instanceof HardLinkRev l) {
            try {
              resolveHardLinkTarget(sys, l);
            } catch (IllegalStateException e) {
              errors.add(sys + ":" + p + "@" + rev + " hard link target missing (" + l.target() + ")");
            }
          }
        }
      }
    }
    if (!errors.isEmpty()) {
      throw new IllegalStateException(String.join("; ", errors));
    }
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

  private static final class MergeMaps<K extends @NonNull Object, V> implements Merger<Map<K, V>> {
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
        V resultVal = result.get(key);
        if (resultVal != null) {
          result.put(key, valueMerger.merge(resultVal, val));
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

  private static final Merger<Map<SystemId, List<BackupMetadata>>> BACKUP_MERGER = new MergeMaps<>(new MergeLists<>(new MergeRequiringEquality<>()));
  private static final Merger<Map<SystemId, Map<Path, List<Revision>>>> FILE_MERGER = new MergeMaps<>(new MergeMaps<>(new MergeLists<>(new MergeRequiringEquality<>())));
  private static final Merger<Map<Sha256AndSize, BackupReport>> BLOB_MERGER = new MergeMaps<>(new MergeWithArbitraryChoice<>());
  private static final Merger<BigInteger> CLEANUP_GENERATION_MERGER = new MergeRequiringEquality<>();

  public BackupIndex merge(BackupIndex other) throws MergeConflict {
    final Map<SystemId, List<BackupMetadata>> backups = BACKUP_MERGER.merge(this.backupHistory, other.backupHistory);
    final Map<SystemId, Map<Path, List<Revision>>> files = FILE_MERGER.merge(this.files, other.files);
    final Map<Sha256AndSize, BackupReport> blobs = BLOB_MERGER.merge(this.blobs, other.blobs);
    final var cleanupGeneration = CLEANUP_GENERATION_MERGER.merge(this.cleanupGeneration, other.cleanupGeneration);
    return new BackupIndex(backups, files, blobs, cleanupGeneration);
  }

}
