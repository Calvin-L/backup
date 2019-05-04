package cal.bkup.impls;

import cal.bkup.types.BackupReport;
import cal.bkup.types.Id;
import cal.bkup.types.Sha256AndSize;
import cal.prim.fs.HardLink;
import cal.prim.fs.SymLink;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>To store an index durably, combine {@link cal.prim.ConsistentBlob} and
 * {@link cal.bkup.types.IndexFormat}.
 *
 * <p>The <code>BackupIndex</code> object does not actually back up files.  Any client code
 * that uses a <code>BackupIndex</code> must ensure that the index's view of the world is
 * honest: if the index reports that a file is backed up to a permanent location, then it
 * really has been.
 */
public class BackupIndex {

  public enum FileType {
    REGULAR_FILE,
    HARD_LINK,
    SOFT_LINK,
    TOMBSTONE
  }

  public static class Revision {
    public final FileType type;

    // type is REGULAR_FILE
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

  private final Map<Id, Map<Path, List<Revision>>> files;
  private final Map<Sha256AndSize, BackupReport> blobs;

  public BackupIndex() {
    this(new HashMap<>(), new HashMap<>());
  }

  private BackupIndex(Map<Id, Map<Path, List<Revision>>> data, Map<Sha256AndSize, BackupReport> blobs) {
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

  public synchronized Set<Id> knownSystems() {
    return ImmutableSet.copyOf(files.keySet());
  }

  public synchronized Set<Path> knownPaths(Id system) {
    Map<Path, List<Revision>> info = files.get(system);
    return info != null ? ImmutableSet.copyOf(info.keySet()) : Collections.emptySet();
  }

  public synchronized List<Revision> getInfo(Id system, Path path) {
    Map<Path, List<Revision>> info = files.get(system);
    if (info == null) {
      return Collections.emptyList();
    }
    List<Revision> revisions = info.get(path);
    return revisions != null ? new ArrayList<>(revisions) : Collections.emptyList();
  }

  public synchronized @Nullable Revision mostRecentRevision(Id system, Path path) {
    List<Revision> revisions = getInfo(system, path);
    if (revisions.isEmpty()) {
      return null;
    }
    return revisions.get(revisions.size() - 1);
  }

  private List<Revision> findOrAddRevisionList(Id system, Path path) {
    return files.computeIfAbsent(system, s -> new HashMap<>())
            .computeIfAbsent(path, p -> new ArrayList<>());
  }

  public synchronized void appendRevision(Id system, Path path, Instant modTime, Sha256AndSize contentSummary) {
    if (lookupBlob(contentSummary) == null) {
      throw new IllegalArgumentException("Refusing to add backed up file that references nonexistent blob " + contentSummary);
    }
    findOrAddRevisionList(system, path).add(new Revision(modTime, contentSummary));
  }

  public synchronized void appendRevision(Id system, Path path, HardLink link) {
    findOrAddRevisionList(system, path).add(new Revision(link));
  }

  public synchronized void appendRevision(Id system, Path path, SymLink link) {
    findOrAddRevisionList(system, path).add(new Revision(link));
  }

  public synchronized void appendTombstone(Id system, Path path) {
    findOrAddRevisionList(system, path).add(new Revision());
  }

  public BackupIndex merge(BackupIndex other) {
    throw new UnsupportedOperationException();
  }

}
