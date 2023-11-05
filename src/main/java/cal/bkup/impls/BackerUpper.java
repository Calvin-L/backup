package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.impls.ProgressDisplay.Task;
import cal.bkup.types.BackupReport;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.NoValue;
import cal.prim.Pair;
import cal.prim.Price;
import cal.prim.QuietAutoCloseable;
import cal.prim.fs.Filesystem;
import cal.prim.fs.HardLink;
import cal.prim.fs.Link;
import cal.prim.fs.RegularFile;
import cal.prim.fs.SymLink;
import cal.prim.storage.ConsistentBlob;
import cal.prim.storage.EventuallyConsistentBlobStore;
import cal.prim.time.MonotonicRealTimeClock;
import cal.prim.time.UnreliableWallClock;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.Encryption;
import cal.prim.transforms.StatisticsCollectingInputStream;
import cal.prim.transforms.TrimmedInputStream;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.MustCall;
import org.checkerframework.checker.mustcall.qual.Owning;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A thing that knows how to back stuff up!
 *
 * <p>Features:
 * <ul>
 *   <li>Agnostic to the underlying storage medium</li>
 *   <li>All stored data is encrypted (the underlying storage never sees unencrypted bytes)</li>
 *   <li>Each file is encrypted with a separate key</li>
 *   <li>One master password encrypts an index containing the keys and other metadata</li>
 * </ul>
 */
@SuppressWarnings({"dereference.of.nullable", "method.invocation", "required.method.not.called", "methodref.return"}) // TODO
public class BackerUpper {

  // Configuration: how do things get stored
  private final CachedBackupIndex indexStore;
  private final BlobTransformer transformer;
  private final EventuallyConsistentBlobStore blobStore;
  private final UnreliableWallClock wallClock;
  private final MonotonicRealTimeClock durationClock = MonotonicRealTimeClock.SYSTEM_CLOCK;
  private final Duration PERIODIC_INDEX_RATE = Duration.ofMinutes(15);

  public BackerUpper(ConsistentBlob indexStore, IndexFormat indexFormat, EventuallyConsistentBlobStore blobStore, BlobTransformer transformer, UnreliableWallClock wallClock) {
    this.indexStore = new CachedBackupIndex(indexStore, indexFormat, transformer);
    this.transformer = transformer;
    this.blobStore = blobStore;
    this.wallClock = wallClock;
  }

  public interface BackupPlan {
    long estimatedBytesUploaded();
    Price estimatedExecutionCost();
    Price estimatedMonthlyCost();
    void execute(Filesystem fs) throws IOException, BackupIndex.MergeConflict;
  }

  public BackupPlan planBackup(
          SystemId whatSystemIsThis,
          String passwordForIndex,
          String newPasswordForIndex,
          StorageCostModel blobStorageCosts,
          Collection<RegularFile> files,
          Collection<SymLink> symlinks,
          Collection<HardLink> hardlinks) throws IOException {

    long bytesUploaded = 0;
    Price totalUploadCost = Price.ZERO;
    Price monthlyStorageCost = Price.ZERO;
    Collection<RegularFile> filteredFiles = new ArrayList<>();
    Collection<Path> toForget = new ArrayList<>();
    Set<Path> inThisBackup = new HashSet<>();

    var index = indexStore.getIndex(passwordForIndex);

    for (RegularFile f : files) {
      inThisBackup.add(f.path());
      BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, f.path());
      Instant lastModTime = (latest instanceof BackupIndex.RegularFileRev latestFile) ? latestFile.modTime() : null;
      Instant localModTime = f.modTime().truncatedTo(ChronoUnit.MILLIS);
      if (!Objects.equals(lastModTime, localModTime)) {
        System.out.println(" --> " + f.path() + " [" + (lastModTime != null ? ("updated: " + lastModTime + " --> ") : "new: ") + localModTime + ']');
        long size = f.sizeInBytes();
        bytesUploaded += size;
        totalUploadCost = totalUploadCost.plus(blobStorageCosts.costToUploadBlob(size));
        monthlyStorageCost = monthlyStorageCost.plus(blobStorageCosts.monthlyStorageCostForBlob(size));
        filteredFiles.add(f);
      }
    }

    for (Link l : symlinks) {
      inThisBackup.add(l.source());
    }

    for (Link l : hardlinks) {
      inThisBackup.add(l.source());
    }

    for (Path p : index.knownPaths(whatSystemIsThis)) {
      if (!inThisBackup.contains(p)) {
        var latest = index.mostRecentRevision(whatSystemIsThis, p);
        assert latest != null;
        if (!(latest instanceof BackupIndex.TombstoneRev)) {
          System.out.println(" --> " + p + " [deleted]");
          toForget.add(p);
        }
      }
    }

    final long finalBytes = bytesUploaded;
    final Price totalPrice = totalUploadCost;
    final Price totalMonthlyPrice = monthlyStorageCost;

    return new BackupPlan() {
      @Override
      public long estimatedBytesUploaded() {
        return finalBytes;
      }

      @Override
      public Price estimatedExecutionCost() {
        return totalPrice;
      }

      @Override
      public Price estimatedMonthlyCost() {
        return totalMonthlyPrice;
      }

      @Override
      public void execute(Filesystem fs) throws IOException, BackupIndex.MergeConflict {
        backup(whatSystemIsThis, passwordForIndex, newPasswordForIndex, fs, filteredFiles, symlinks, hardlinks, toForget);
      }
    };

  }

  @VisibleForTesting
  public void backup(SystemId whatSystemIsThis, String passwordForIndex, String newPasswordForIndex, Filesystem fs, Collection<RegularFile> files, Collection<SymLink> symlinks, Collection<HardLink> hardlinks, Collection<Path> toForget) throws IOException, BackupIndex.MergeConflict {

    List<String> warnings = new ArrayList<>();
    final AtomicBoolean keepRunning = new AtomicBoolean(true);

    String currentPassword = passwordForIndex;
    try (var ignored1 = Util.catchShutdown(() -> {
      System.err.println("Shutting down...");
      keepRunning.set(false);
    })) {
      BackupIndex.BackupMetadata backupId = null;

      try (ProgressDisplay progress = new ProgressDisplay(files.size() * 2L + symlinks.size() + hardlinks.size() + toForget.size());
           var txn = indexStore.beginTransaction(currentPassword)) {

        var index = txn.getIndex();

        var lastIndexSave = durationClock.sample();
        backupId = index.startBackup(whatSystemIsThis, wallClock.now());

        var remainingFiles = new LinkedHashSet<>(files);
        while (!remainingFiles.isEmpty()) {
          if (!keepRunning.get()) {
            return;
          }

          var batch = nextBatch(fs, remainingFiles, whatSystemIsThis, backupId, index, progress, warnings);
          var uploadResult = uploadBatch(fs, batch, progress);
          for (var entry : uploadResult.entrySet()) {
            var f = entry.getKey();
            var summary = entry.getValue().fst();
            var report = entry.getValue().snd();
            var actualReport = index.addOrCanonicalizeBackedUpBlob(summary, report);
            if (!actualReport.equals(report)) {
              warnings.add("Duplicate file detected in batch " + report.idAtTarget() + " (" + report.sizeAtTarget() + " junk bytes were uploaded)");
            }
            index.appendRevision(whatSystemIsThis, backupId, f.path(), f.modTime(), summary);
          }

          var now = durationClock.sample();
          if (Util.ge(durationClock.timeBetweenSamples(lastIndexSave, now), PERIODIC_INDEX_RATE)) {
            txn.commit(newPasswordForIndex, CachedBackupIndex.ConflictBehavior.TRY_MERGE);
          }
        }

        for (var link : symlinks) {
          Task task = progress.startTask("Adding soft link " + link.source() + " --> " + link.destination());
          BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.source());
          if (latest instanceof BackupIndex.SoftLinkRev linkRev && Objects.equals(linkRev.target(), link.destination())) {
            progress.finishTask(task);
            continue;
          }
          index.appendRevision(whatSystemIsThis, backupId, link.source(), link);
          progress.finishTask(task);
        }

        for (var link : hardlinks) {
          Task task = progress.startTask("Adding hard link " + link.source() + " --> " + link.destination());
          BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.source());
          if (latest instanceof BackupIndex.HardLinkRev linkRev && Objects.equals(linkRev.target(), link.destination())) {
            progress.finishTask(task);
            continue;
          }
          index.appendRevision(whatSystemIsThis, backupId, link.source(), link);
          progress.finishTask(task);
        }

        for (Path p : toForget) {
          Task task = progress.startTask("Removing from backup " + p);
          index.appendTombstone(whatSystemIsThis, backupId, p);
          progress.finishTask(task);
        }

        index.finishBackup(whatSystemIsThis, backupId, wallClock.now());
        txn.commit(newPasswordForIndex, CachedBackupIndex.ConflictBehavior.TRY_MERGE);
      } finally {
        if (warnings.size() > 0) {
          System.out.println(warnings.size() + " warnings:");
          for (String w : warnings) {
            System.out.print(" - ");
            System.out.println(w);
          }
        }
      }
    }
  }

  /**
   * Select a batch of files to group together.
   * Removes them from <code>files</code>, along with any that should be skipped.
   * Files may be skipped if they have disappeared or if they are already present
   * in the <code>index</code>.
   */
  private Collection<Pair<RegularFile, Sha256AndSize>> nextBatch(Filesystem fs, Collection<RegularFile> files, SystemId systemId, BackupIndex.BackupMetadata backupId, BackupIndex index, ProgressDisplay display, List<String> warnings) throws IOException {
    final long PREFERRED_BATCH_SIZE_IN_BYTES = 10_000_000_000L; // 10 GB

    long total = 0;
    var seen = new HashSet<Sha256AndSize>();
    var result = new ArrayList<Pair<RegularFile, Sha256AndSize>>();
    Iterator<RegularFile> it = files.iterator();
    while (it.hasNext()) {
      var f = it.next();
      if (result.isEmpty() || total + f.sizeInBytes() < PREFERRED_BATCH_SIZE_IN_BYTES) {
        Sha256AndSize summary = null;
        try {
          summary = summarize(fs, f, display);
        } catch (NoSuchFileException exn) {
          warnings.add("File " + f.path() + " disappeared between planning and checksum");
        }
        if (summary != null) {
          boolean isNewToThisBatch = seen.add(summary);
          if (!isNewToThisBatch) {
            // defer to next batch
            continue;
          }
          if (index.lookupBlob(summary) == null) {
            if (f.sizeInBytes() != summary.size()) {
              warnings.add("File " + f.path() + " changed size between planning and checksum");
            }
            result.add(new Pair<>(f, summary));
            total += summary.size();
          } else {
            index.appendRevision(systemId, backupId, f.path(), f.modTime(), summary);
            display.skipTask("Upload " + f.path(), "data is already present");
          }
        }
        it.remove();
      }
    }

    return result;
  }

  private Sha256AndSize summarize(Filesystem fs, RegularFile f, ProgressDisplay display) throws IOException {
    Task checksumTask = display.startTask("checksum " + f.path());
    try (InputStream in = Util.buffered(fs.openRegularFileForReading(f.path()))) {
      return Util.summarize(in, stream ->
              display.reportProgress(checksumTask, stream.getBytesRead(), f.sizeInBytes()));
    } finally {
      display.finishTask(checksumTask);
    }
  }

  public static class BackedUpThing {
    private final SystemId system;
    private final Path path;
    private final BackupIndex.Revision latestRevision;

    public BackedUpThing(SystemId system, Path path, BackupIndex.Revision latestRevision) {
      this.system = system;
      this.path = path;
      this.latestRevision = latestRevision;
    }

    public SystemId system() {
      return system;
    }

    public Path path() {
      return path;
    }

    public BackupIndex.Revision latestRevision() {
      return latestRevision;
    }
  }

  public Stream<BackedUpThing> list(String indexPassword) throws IOException {
    var index = indexStore.getIndex(indexPassword);
    return index.knownSystems().stream().flatMap(system -> index.knownPaths(system).stream().map(path -> {
      BackupIndex.Revision r = index.mostRecentRevision(system, path);
      assert r != null : "@AssumeAssertion(nullness): known path must have at least 1 known revision";
      return new BackedUpThing(system, path, r);
    }));
  }

  public InputStream restore(String indexPassword, Sha256AndSize blob) throws IOException {
    var index = indexStore.getIndex(indexPassword);
    BackupReport report = index.lookupBlob(blob);
    if (report == null) {
      throw new NoSuchElementException();
    }
    InputStream s = Util.buffered(blobStore.open(report.idAtTarget()));
    try {
      s.skipNBytes(report.offsetAtTarget());
      s = new TrimmedInputStream(s, report.sizeAtTarget());
      return transformer.followedBy(new Encryption(report.key())).unApply(s);
    } catch (Exception e) {
      s.close();
      throw e;
    }
  }

  public interface CleanupPlan extends QuietAutoCloseable {
    long totalBlobsReclaimed();
    long untrackedBlobsReclaimed();
    long bytesReclaimed();
    Price estimatedExecutionCost();
    Price estimatedMonthlyCost();
    void execute() throws IOException, BackupIndex.MergeConflict;
  }

  private static boolean isSafeToForget(BackupIndex index, SystemId system, Path path, BackupIndex.Revision revision, long earliestBackupToKeep) {

    // Diagram explaining hardlinks.
    //
    //                            earliestBackupToKeep
    //                                     v
    //      |    R1    |      R2     |     R3
    // ---------------------------------------------
    //      |          |             |
    //  /a  |   file   |             |    file'
    //      |          |             |
    //  /b  |          |   hardlink  |
    //      |          |    to /a    |
    //
    // The hardlink /b@R2 is NOT safe to forget because it does not
    // have a newer version in R3.  However, /a@R1 IS safe to forget,
    // even though /b@R2 refers to it, because in R3 /b actually refers
    // to /a@R3, which will not be forgotten.

    return revision.backupNumber() < earliestBackupToKeep
            && index.getInfo(system, path).stream().anyMatch(r -> revision.backupNumber() < r.backupNumber() && r.backupNumber() <= earliestBackupToKeep)
//            && index.findHardLinksPointingTo(system, path, revision.backupNumber).stream().allMatch(hardlink -> isSafeToForget(index, system, path, hardlink, earliestBackupToKeep))
            ;
  }

  private record BackupID(
    SystemId system,
    BackupIndex.BackupMetadata info) {
  }

  private record RevisionID(
    SystemId system,
    Path path,
    BackupIndex.Revision revision) {
  }

  public CleanupPlan planCleanup(String password, Duration retentionTime, StorageCostModel blobStorageCosts) throws IOException {
    var cutoff = wallClock.now().minus(retentionTime);
    var txn = indexStore.beginTransaction(password);
    var index = txn.getIndex();

    // NOTE: There are several kinds of blobs in `blobsExistingAtStart`:
    //  (1) "Orphaned" blobs that were created by failed backups.  These
    //      are safe to delete.
    //  (2) "In-use" blobs that were referenced by the index.  These are
    //      safe to delete if they are no longer referenced by the index.
    //  (3) "Usage-pending" blobs that may or may not have been referenced
    //      by the index, and are referenced by in-progress backups unknown
    //      to this cleanup process.  These are safe to delete since we
    //      bumped the cleanup generation and saved the index successfully:
    //      pending backups cannot complete successfully.
    Set<String> blobsExistingAtStart = blobStore.list().collect(Collectors.toSet());
    Set<RevisionID> revisionsAtStart = new LinkedHashSet<>(); // collected below
    Set<BackupID> backupsAtStart = index.knownSystems().stream()
        .flatMap(sys -> index.knownBackups(sys).stream().map(b -> new BackupID(sys, b)))
        .collect(Collectors.toSet());

    // "Mark" -- find everything reachable
    Set<RevisionID> reachableRevisions = new LinkedHashSet<>();
    Set<BackupID> reachableBackups = new LinkedHashSet<>();
    Set<SystemId> reachableSystems = new LinkedHashSet<>();
    Set<String> reachableBlobs = new LinkedHashSet<>();
    Set<Sha256AndSize> reachableSummaries = new LinkedHashSet<>();

    Queue<RevisionID> revisionsToVisit = new ArrayDeque<>();
    for (SystemId system : index.knownSystems()) {
      var backups = index.knownBackups(system);
      if (backups.size() > 0) {
        long earliestBackupToKeep = backups.stream().max(Comparator.comparingLong(BackupIndex.BackupMetadata::backupNumber)).get().backupNumber();
        for (BackupIndex.BackupMetadata backupInfo : backups) {
          if (backupInfo.startTime().compareTo(cutoff) >= 0) {
            earliestBackupToKeep = Math.min(earliestBackupToKeep, backupInfo.backupNumber());
          }
        }

        for (Path path : Util.sorted(index.knownPaths(system))) {
          for (BackupIndex.Revision revision : index.getInfo(system, path)) {
            var revID = new RevisionID(system, path, revision);
            revisionsAtStart.add(revID);
            if (!(revision instanceof BackupIndex.TombstoneRev) && !isSafeToForget(index, system, path, revision, earliestBackupToKeep)) {
              revisionsToVisit.add(revID);
            }
          }
        }
      }
    }

    while (!revisionsToVisit.isEmpty()) {
      var revID = revisionsToVisit.remove();
      var system = revID.system();
      if (reachableRevisions.add(revID)) {
        index.knownBackups(system).stream()
            .filter(backup -> backup.backupNumber() == revID.revision().backupNumber())
            .map(info -> new BackupID(system, info))
            .forEach(reachableBackups::add);
        reachableSystems.add(system);
        if (revID.revision() instanceof BackupIndex.RegularFileRev f) {
          var info = index.lookupBlob(f.summary());
          if (info == null) {
            throw new IllegalStateException("No blob available for file revision " + f);
          }
          reachableBlobs.add(info.idAtTarget());
          reachableSummaries.add(f.summary());
        } else if (revID.revision() instanceof BackupIndex.HardLinkRev l) {
          var target = index.resolveHardLinkTarget(system, l);
          revisionsToVisit.add(new RevisionID(system, l.target(), target));
        }

        // If this is a non-tombstone, then the next tombstone after it is also reachable.
        if (!(revID.revision() instanceof BackupIndex.TombstoneRev)) {
          Optional<BackupIndex.Revision> maybeNextRevision = index.getInfo(system, revID.path()).stream()
              .filter(rev -> rev.backupNumber() > revID.revision().backupNumber())
              .min(Comparator.comparingLong(BackupIndex.Revision::backupNumber));
          if (maybeNextRevision.isPresent()) {
            var nextRevision = maybeNextRevision.get();
            if (nextRevision instanceof BackupIndex.TombstoneRev) {
              revisionsToVisit.add(new RevisionID(system, revID.path(), nextRevision));
            }
          }
        }
      }
    }

    // "Sweep" -- anything that isn't referenced can be deleted
    Set<SystemId> systemsToForget = Sets.difference(index.knownSystems(), reachableSystems);
    Set<BackupID> backupsToForget = Sets.difference(backupsAtStart, reachableBackups);
    Set<RevisionID> revisionsToForget = Sets.difference(revisionsAtStart, reachableRevisions);
    Set<String> blobsToDelete = Sets.difference(blobsExistingAtStart, reachableBlobs);
    Set<Sha256AndSize> summariesToForget = Sets.difference(index.listBlobs().collect(Collectors.toSet()), reachableSummaries);

    Multimap<String, BackupReport> backedUpBlobsByIDAtTarget = index.listBlobs()
            .map(index::lookupBlob)
            .map(Objects::requireNonNull)
            .collect(ArrayListMultimap::create, (m, i) -> m.put(i.idAtTarget(), i), Multimap::putAll);

    return new CleanupPlanImpl(blobsToDelete, backedUpBlobsByIDAtTarget, blobStorageCosts, revisionsToForget, backupsToForget, systemsToForget, summariesToForget, txn, password);
  }

  // -------------------------------------------------------------------------
  // Helpers for doing backups

  private Map<RegularFile, Pair<Sha256AndSize, BackupReport>> uploadBatch(Filesystem fs, Collection<Pair<RegularFile, Sha256AndSize>> files, ProgressDisplay display) throws IOException {
    if (files.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<Path, Sha256AndSize> actualSummaries = new HashMap<>();
    Map<Path, Long> offsetsAtTarget = new HashMap<>();
    Map<Path, Long> sizesAtTarget = new HashMap<>();

    String key = Util.randomPassword();

    EventuallyConsistentBlobStore.PutResult result;
    try (InputStream is = new InputStream() {
      long bytesSent = 0L;
      final Iterator<Pair<RegularFile, Sha256AndSize>> remaining = files.iterator();
      @Nullable RegularFile currentFile = null;
      long startOffsetOfCurrentFile;
      @Nullable StatisticsCollectingInputStream stats = null;
      @Nullable InputStream current;
      @Nullable Task task = null;

      @SuppressWarnings("argument") // TODO
      final Consumer<@MustCall({}) StatisticsCollectingInputStream> reportProgress = s ->
              display.reportProgress(task, s.getBytesRead(), currentFile.sizeInBytes());

      {
        openNext();
      }

      private void closeCurrent() throws IOException {
        if (current != null) {
          try {
            current.close();
            assert stats.isClosed();
            var path = currentFile.path();
            offsetsAtTarget.put(path, startOffsetOfCurrentFile);
            sizesAtTarget.put(path, bytesSent - startOffsetOfCurrentFile);
            actualSummaries.put(path, new Sha256AndSize(
                    stats.getSha256Digest(),
                    stats.getBytesRead()));
          } catch (Exception e) {
            e.printStackTrace();
            throw e;
          }
          if (task != null) {
            display.finishTask(task);
          }
        }
      }

      private void openNext() throws IOException {
        if (remaining.hasNext()) {
          currentFile = remaining.next().fst();
          task = display.startTask("Upload " + currentFile.path());
          startOffsetOfCurrentFile = bytesSent;
          var newStats = new StatisticsCollectingInputStream(Util.buffered(fs.openRegularFileForReading(currentFile.path())), reportProgress);
          stats = newStats;
          current = transformer.followedBy(new Encryption(key)).apply(newStats);
        } else {
          current = null;
        }
      }

      @Override
      public int read() throws IOException {
        while (current != null) {
          int nextByte = current.read();
          if (nextByte < 0) {
            closeCurrent();
            openNext();
          } else {
            ++bytesSent;
            return nextByte;
          }
        }
        return -1;
      }

      @Override
      public void close() {
        if (current != null) {
          throw new RuntimeException("not all of the stream was consumed");
        }
      }
    }) {
      result = blobStore.put(is);
    }

    // Sanity check: sum of sizes should equal total upload size
    long expectedSize = sizesAtTarget.values().stream().mapToLong(l -> l).sum();
    long actualSize = result.bytesStored();
    if (expectedSize != actualSize) {
      throw new RuntimeException("Batch upload seems to have been corrupted: " + actualSize + " bytes were uploaded, but I expected " + expectedSize);
    }

    var finalReports = new HashMap<RegularFile, Pair<Sha256AndSize, BackupReport>>();
    for (var pair : files) {
      var f = pair.fst();
      var path = f.path();

      Long offsetAtTarget = offsetsAtTarget.get(path);
      if (offsetAtTarget == null) {
        throw new IllegalStateException("No target offset for " + path);
      }

      Long sizeAtTarget = sizesAtTarget.get(path);
      if (sizeAtTarget == null) {
        throw new IllegalStateException("No target size for " + path);
      }

      Sha256AndSize summary = actualSummaries.get(path);
      if (summary == null) {
        throw new IllegalStateException("No summary for " + path);
      }

      finalReports.put(f, new Pair<>(
              summary,
              new BackupReport(
                      result.identifier(),
                      offsetAtTarget,
                      sizeAtTarget,
                      key)));
    }

    return finalReports;
  }

  public BackupIndex getIndex(String password) throws IOException {
    return indexStore.getIndex(password);
  }

  public InputStream readRawIndex(String password) throws IOException, NoValue, ConsistentBlob.TagExpired {
    return indexStore.readRawIndex(password);
  }

  private class CleanupPlanImpl implements CleanupPlan {
    private final Set<String> blobsToDelete;
    private final Multimap<String, BackupReport> backedUpBlobsByIDAtTarget;
    private final StorageCostModel blobStorageCosts;
    private final Set<RevisionID> revisionsToForget;
    private final Set<BackupID> backupsToForget;
    private final Set<SystemId> systemsToForget;
    private final Set<Sha256AndSize> summariesToForget;
    private final @Owning CachedBackupIndex.Transaction txn;
    private final String password;

    public CleanupPlanImpl(Set<String> blobsToDelete, Multimap<String, BackupReport> backedUpBlobsByIDAtTarget, StorageCostModel blobStorageCosts, Set<RevisionID> revisionsToForget, Set<BackupID> backupsToForget, Set<SystemId> systemsToForget, Set<Sha256AndSize> summariesToForget, CachedBackupIndex.Transaction txn, String password) {
      this.blobsToDelete = blobsToDelete;
      this.backedUpBlobsByIDAtTarget = backedUpBlobsByIDAtTarget;
      this.blobStorageCosts = blobStorageCosts;
      this.revisionsToForget = revisionsToForget;
      this.backupsToForget = backupsToForget;
      this.systemsToForget = systemsToForget;
      this.summariesToForget = summariesToForget;
      this.txn = txn;
      this.password = password;
    }

    @Override
    public long totalBlobsReclaimed() {
      return blobsToDelete.size();
    }

    @Override
    public long untrackedBlobsReclaimed() {
      var index = txn.getIndex();
      Set<String> blobsKnownToIndex = index.listBlobs()
          .map(index::lookupBlob)
          .map(Objects::requireNonNull)
          .map(BackupReport::idAtTarget)
          .collect(Collectors.toSet());
      return blobsToDelete.stream().filter(b -> !blobsKnownToIndex.contains(b)).count();
    }

    @Override
    public long bytesReclaimed() {
      return blobsToDelete.stream()
              .map(backedUpBlobsByIDAtTarget::get)
              .filter(Objects::nonNull)
              .flatMap(Collection::stream)
              .mapToLong(BackupReport::sizeAtTarget)
              .sum();
    }

    @Override
    public Price estimatedExecutionCost() {
      return blobsToDelete.stream()
              .map(backedUpBlobsByIDAtTarget::get)
              .filter(Objects::nonNull)
              .flatMap(Collection::stream)
              .map(report -> blobStorageCosts.costToDeleteBlob(report.sizeAtTarget(), Duration.ZERO))
              .reduce(Price.ZERO, Price::plus);
    }

    @Override
    public Price estimatedMonthlyCost() {
      return blobsToDelete.stream()
              .map(backedUpBlobsByIDAtTarget::get)
              .filter(Objects::nonNull)
              .flatMap(Collection::stream)
              .mapToLong(BackupReport::sizeAtTarget)
              .mapToObj(blobStorageCosts::monthlyStorageCostForBlob)
              .reduce(Price.ZERO, Price::plus)
              .negate();
    }

    @Override
    public void execute() throws IOException, BackupIndex.MergeConflict {
      System.out.println("Cleaning up index");
      var index = txn.getIndex();
      index.checkIntegrity();

      for (RevisionID rev : revisionsToForget) {
        index.forgetRevision(rev.system, rev.path, rev.revision);
      }
      for (BackupID backup : backupsToForget) {
        index.forgetBackup(backup.system, backup.info);
      }
      for (SystemId system : systemsToForget) {
        index.forgetSystem(system);
      }
      for (Sha256AndSize blob : summariesToForget) {
        index.forgetBlob(blob);
      }

      index.checkIntegrity();

      System.out.println("Bumping cleanup generation and saving index");
      index.bumpCleanupGeneration();
      txn.commit(password, CachedBackupIndex.ConflictBehavior.ALWAYS_FAIL);

      System.out.println("Cleaning up the index store");
      indexStore.cleanup(true);

      System.out.println("Deleting " + blobsToDelete.size() + " blobs");
      for (String blobName : blobsToDelete) {
        System.out.println(" --> " + blobName);
        blobStore.delete(blobName);
      }
    }

    @Override
    @EnsuresCalledMethods(value = "txn", methods = {"close"})
    public void close() {
      txn.close();
    }
  }
}
