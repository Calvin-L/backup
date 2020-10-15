package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.MalformedDataException;
import cal.prim.NoValue;
import cal.prim.Pair;
import cal.prim.PreconditionFailed;
import cal.prim.Price;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
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
public class BackerUpper {

  // Configuration: how do things get stored
  private final ConsistentBlob indexStore;
  private final IndexFormat indexFormat;
  private final EventuallyConsistentBlobStore blobStore;
  private final BlobTransformer transformer;
  private final UnreliableWallClock wallClock;
  private final MonotonicRealTimeClock durationClock = MonotonicRealTimeClock.SYSTEM_CLOCK;
  private final Duration PERIODIC_INDEX_RATE = Duration.ofMinutes(15);

  // Cached data
  private BackupIndex index = null;
  private ConsistentBlob.Tag tagForLastIndexLoad = null;

  public BackerUpper(ConsistentBlob indexStore, IndexFormat indexFormat, EventuallyConsistentBlobStore blobStore, BlobTransformer transformer, UnreliableWallClock wallClock) {
    this.indexStore = indexStore;
    this.indexFormat = indexFormat;
    this.blobStore = blobStore;
    this.transformer = transformer;
    this.wallClock = wallClock;
  }

  public interface BackupPlan {
    long estimatedBytesUploaded();
    Price estimatedExecutionCost();
    Price estimatedMonthlyCost();
    void execute() throws IOException, BackupIndex.MergeConflict;
  }

  public BackupPlan planBackup(
          SystemId whatSystemIsThis,
          String passwordForIndex,
          String newPasswordForIndex,
          StorageCostModel blobStorageCosts,
          Collection<RegularFile> files,
          Collection<SymLink> symlinks,
          Collection<HardLink> hardlinks) throws IOException {

    loadIndexIfMissing(passwordForIndex);

    long bytesUploaded = 0;
    Price totalUploadCost = Price.ZERO;
    Price monthlyStorageCost = Price.ZERO;
    Collection<RegularFile> filteredFiles = new ArrayList<>();
    Collection<Path> toForget = new ArrayList<>();
    Set<Path> inThisBackup = new HashSet<>();

    for (RegularFile f : files) {
      inThisBackup.add(f.getPath());
      BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, f.getPath());
      Instant lastModTime = (latest != null && latest.type == BackupIndex.FileType.REGULAR_FILE) ? latest.modTime : null;
      Instant localModTime = f.getModTime().truncatedTo(ChronoUnit.MILLIS);
      if (!Objects.equals(lastModTime, localModTime)) {
        System.out.println(" --> " + f.getPath() + " [" + (lastModTime != null ? ("updated: " + lastModTime + " --> ") : "new: ") + localModTime + ']');
        long size = f.getSizeInBytes();
        bytesUploaded += size;
        totalUploadCost = totalUploadCost.plus(blobStorageCosts.costToUploadBlob(size));
        monthlyStorageCost = monthlyStorageCost.plus(blobStorageCosts.monthlyStorageCostForBlob(size));
        filteredFiles.add(f);
      }
    }

    for (Link l : symlinks) {
      inThisBackup.add(l.getSource());
    }

    for (Link l : hardlinks) {
      inThisBackup.add(l.getSource());
    }

    for (Path p : index.knownPaths(whatSystemIsThis)) {
      if (!inThisBackup.contains(p)) {
        var latest = index.mostRecentRevision(whatSystemIsThis, p);
        assert latest != null;
        if (latest.type != BackupIndex.FileType.TOMBSTONE) {
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
      public void execute() throws IOException, BackupIndex.MergeConflict {
        backup(whatSystemIsThis, passwordForIndex, newPasswordForIndex, filteredFiles, symlinks, hardlinks, toForget);
      }
    };

  }

  public void backup(SystemId whatSystemIsThis, String passwordForIndex, String newPasswordForIndex, Collection<RegularFile> files, Collection<SymLink> symlinks, Collection<HardLink> hardlinks, Collection<Path> toForget) throws IOException, BackupIndex.MergeConflict {

    List<String> warnings = new ArrayList<>();
    final AtomicBoolean keepRunning = new AtomicBoolean(true);

    String currentPassword = passwordForIndex;
    try (var ignored1 = Util.catchShutdown(() -> {
      System.err.println("Shutting down...");
      keepRunning.set(false);
    })) {
      BackupIndex.BackupMetadata backupId = null;

      try (ProgressDisplay progress = new ProgressDisplay(files.size() * 2 + symlinks.size() + hardlinks.size() + toForget.size())) {
        loadIndexIfMissing(currentPassword);

        var lastIndexSave = durationClock.sample();
        backupId = index.startBackup(whatSystemIsThis, wallClock.now());

        for (var f : files) {
          if (!keepRunning.get()) {
            return;
          }
          try {
            uploadAndAddToIndex(index, whatSystemIsThis, backupId, f, progress);
          } catch (NoSuchFileException ignored2) {
            warnings.add("The file " + f.getPath() + " was deleted before it could be backed up");
          }
          var now = durationClock.sample();
          if (Util.ge(durationClock.timeBetweenSamples(lastIndexSave, now), PERIODIC_INDEX_RATE)) {
            saveIndex(currentPassword, newPasswordForIndex);
            currentPassword = newPasswordForIndex;
            lastIndexSave = now;
          }
        }

        for (var link : symlinks) {
          ProgressDisplay.Task task = progress.startTask("Adding soft link " + link.getSource() + " --> " + link.getDestination());
          BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.getSource());
          if (latest != null && latest.type == BackupIndex.FileType.SOFT_LINK && Objects.equals(latest.linkTarget, link.getDestination())) {
            progress.finishTask(task);
            continue;
          }
          index.appendRevision(whatSystemIsThis, backupId, link.getSource(), link);
          progress.finishTask(task);
        }

        for (var link : hardlinks) {
          ProgressDisplay.Task task = progress.startTask("Adding hard link " + link.getSource() + " --> " + link.getDestination());
          BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.getSource());
          if (latest != null && latest.type == BackupIndex.FileType.HARD_LINK && Objects.equals(latest.linkTarget, link.getDestination())) {
            progress.finishTask(task);
            continue;
          }
          index.appendRevision(whatSystemIsThis, backupId, link.getSource(), link);
          progress.finishTask(task);
        }

        for (Path p : toForget) {
          ProgressDisplay.Task task = progress.startTask("Removing from backup " + p);
          index.appendTombstone(whatSystemIsThis, backupId, p);
          progress.finishTask(task);
        }
      } finally {
        try {
          if (backupId != null) {
            index.finishBackup(whatSystemIsThis, backupId, wallClock.now());
            saveIndex(currentPassword, newPasswordForIndex);
          }
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
    loadIndexIfMissing(indexPassword);
    return index.knownSystems().stream().flatMap(system -> index.knownPaths(system).stream().map(path -> {
      BackupIndex.Revision r = index.mostRecentRevision(system, path);
      return new BackedUpThing(system, path, r);
    }));
  }

  public InputStream restore(String indexPassword, Sha256AndSize blob) throws IOException {
    loadIndexIfMissing(indexPassword);
    BackupReport report = index.lookupBlob(blob);
    if (report == null) {
      throw new NoSuchElementException();
    }
    return transformer.followedBy(new Encryption(report.getKey())).unApply(blobStore.open(report.getIdAtTarget()));
  }

  /**
   * Clean up old state.  This method calls <code>{@link ConsistentBlob#cleanup(boolean) cleanup}(forReal)</code>
   * on the index store.  It also deletes snapshots of backed-up files that are older than the given
   * <code>retentionTime</code>.
   *
   * @param forReal true to actually clean up, or false for a "dry run" that only prints what would be done
   * @param password the password to decrypt/encrypt the index
   * @param retentionTime how far back to keep snapshots of files
   * @throws IOException if an error occurs during cleanup.  This is an "ambiguous" outcome: the cleanup might
   *   only partially happen.  Some of the cleanup that was incomplete might happen in the future.  This
   *   exception can be thrown even if <code>forReal == false</code>, indicating that something went wrong
   *   while determining what can be cleaned up.
   */
  public void cleanup(boolean forReal, String password, Duration retentionTime) throws IOException {
    indexStore.cleanup(forReal);
    loadIndexIfMissing(password);
    var cutoff = wallClock.now().minus(retentionTime);

    for (SystemId system : index.knownSystems()) {
      for (Path path : Util.sorted(index.knownPaths(system))) {
        var revisions = index.getInfo(system, path);
        int numToForget = 0;
        for (int i = 0; i < revisions.size(); ++i) {
          if (!canCleanup(revisions, i, cutoff)) {
            break;
          }
          var revision = revisions.get(i);
          System.out.print("cleaning up " + path);
          switch (revision.type) {
            case REGULAR_FILE:
              System.out.println(" (regular file, last modified " + revision.modTime + ')');
              var nextRevision = revisions.get(i+1);
              if (nextRevision.type.equals(BackupIndex.FileType.REGULAR_FILE)) {
                System.out.println("  --> shasum " +
                        Util.sha256toString(revision.summary.getSha256()) + " --> " +
                        Util.sha256toString(nextRevision.summary.getSha256()));
              } else {
                System.out.println("  --> next revision " + nextRevision);
              }
              break;
            case HARD_LINK:
              System.out.println(" (hard link to " + revision.linkTarget + ')');
              break;
            case SOFT_LINK:
              System.out.println(" (soft link to " + revision.linkTarget + ')');
              break;
            case TOMBSTONE:
              System.out.println(" (tombstone marker)");
              break;
            default:
              System.out.println();
              break;
          }
          ++numToForget;
        }

        // TODO: this is wrong!
        // (1) We might delete the last reference to a file when there is a hard link
        //     to its contents...?
        // (2) We can't safely delete any blobs concurrently!

//        if (forReal) {
//          for (int i = 0; i < numToForget; ++i) {
//            index.forgetOldestRevision(system, path);
//          }
//        }

      }
    }

    // TODO: prune old data from BackupIndex
    // TODO: when can we delete things in the blob store?
    throw new UnsupportedOperationException();
  }

  /**
   * Assuming that revisions 0 to <code>i-1</code> (inclusive) are
   * going to be cleaned up, can revision <code>i</code> be cleaned
   * up?
   *
   * @param revisions a list of revisions to consider
   * @param i the index of the revision to check
   * @param cutoff the "cutoff" time: revisions newer than this must be kept
   * @return whether <code>revisions[i]</code> can be cleaned up
   */
  private boolean canCleanup(List<BackupIndex.Revision> revisions, int i, Instant cutoff) {
    var revision = revisions.get(i);
    boolean hasNextRevision = i < (revisions.size() - 1);
    switch (revision.type) {
      case REGULAR_FILE:
        return revision.modTime.isBefore(cutoff) && hasNextRevision;
      case HARD_LINK:
      case SOFT_LINK:
        return hasNextRevision;
      case TOMBSTONE:
        return true;
      default:
        throw new IllegalArgumentException("unknown type for revision " + revision);
    }
  }

  // -------------------------------------------------------------------------
  // Helpers for doing backups

  /**
   *
   * @param index
   * @param systemId
   * @param f
   * @param display
   * @throws NoSuchFileException if another process deletes the file before or during upload
   * @throws IOException
   */
  private void uploadAndAddToIndex(BackupIndex index, SystemId systemId, BackupIndex.BackupMetadata backupId, RegularFile f, ProgressDisplay display) throws IOException {
    // (1) Get the modification time.
    // This has to happen before computing the checksum.  Otherwise,
    // we might miss a new revision of the file on a future backup.
    // This could happen if the file changes after computing the
    // checksum but before reading the modification time.
    Instant modTime = f.getModTime();

    // (2) Checksum the path.
    ProgressDisplay.Task checksumTask = display.startTask("checksum " + f.getPath());
    Sha256AndSize summary;
    try (InputStream in = Util.buffered(f.open())) {
      summary = Util.summarize(in, stream -> {
        display.reportProgress(checksumTask, stream.getBytesRead(), f.getSizeInBytes());
      });
    } finally {
      display.finishTask(checksumTask);
    }

    // (3) Is this file known to the index?  Then no problem!
    BackupReport report = index.lookupBlob(summary);

    // (4) Not known?  Upload it!
    String uploadDescription = "upload " + f.getPath();
    if (report == null) {
      ProgressDisplay.Task task = display.startTask(uploadDescription);
      String key = Util.randomPassword();
      Consumer<StatisticsCollectingInputStream> reportProgress = s -> {
        display.reportProgress(task, s.getBytesRead(), f.getSizeInBytes());
      };
      String identifier;
      long sizeAtTarget;
      StatisticsCollectingInputStream in = new StatisticsCollectingInputStream(Util.buffered(f.open()), reportProgress);
      try (InputStream transformed = transformer.followedBy(new Encryption(key)).apply(in)) {
        EventuallyConsistentBlobStore.PutResult result = blobStore.put(transformed);
        identifier = result.getIdentifier();
        sizeAtTarget = result.getBytesStored();
      } finally {
        display.finishTask(task); // TODO: print identifier and byte count?
      }
      assert in.isClosed();
      long size = in.getBytesRead();
      byte[] sha256 = in.getSha256Digest();
      report = new BackupReport(identifier, sizeAtTarget, key);
      summary = new Sha256AndSize(sha256, size);
      index.addBackedUpBlob(summary, report);
    } else {
      display.skipTask(uploadDescription);
    }

    // Record the latest details.
    index.appendRevision(systemId, backupId, f.getPath(), modTime, summary);
  }

  // -------------------------------------------------------------------------
  // Helpers for managing the index

  private Pair<ConsistentBlob.Tag, BackupIndex> readLatestFromIndexStore(String password) throws IOException {
    for (;;) {
      var tag = indexStore.head();
      BackupIndex index;
      System.out.println("Reading index " + tag + "...");
      try (InputStream in = transformer.followedBy(new Encryption(password)).unApply(indexStore.read(tag))) {
        index = indexFormat.load(in);
        System.out.println(" *** read index");
      } catch (NoValue noValue) {
        System.out.println("No index was found; creating a new one");
        index = new BackupIndex();
      } catch (ConsistentBlob.TagExpired ignored) {
        System.out.println("The tag expired; retrying...");
        continue;
      } catch (MalformedDataException e) {
        throw new IllegalStateException("The index appears to have become corrupt!", e);
      }
      return new Pair<>(tag, index);
    }
  }

  private void loadIndexIfMissing(String password) throws IOException {
    if (index == null) {
      var tagAndIndex = readLatestFromIndexStore(password);
      tagForLastIndexLoad = tagAndIndex.getFst();
      index = tagAndIndex.getSnd();
    }
  }

  public InputStream readRawIndex(String password) throws IOException, NoValue, ConsistentBlob.TagExpired {
    return transformer.followedBy(new Encryption(password)).unApply(indexStore.read(indexStore.head()));
  }

  /**
   * Commit the contents of {@link #index} to {@link #indexStore}.
   * If another process has modified the index stored in {@link #indexStore},
   * this method will download it, merge the other process's
   * modifications with this one, and try again.
   *
   * @param currentPassword the old password, used if the index needs to be downloaded
   * @param newPassword the password to encrypt the index
   * @throws IOException
   * @throws cal.bkup.impls.BackupIndex.MergeConflict if another process modified the index, but the changes
   *         cannot be merged with the changes made by this process
   */
  private void saveIndex(String currentPassword, String newPassword) throws IOException, BackupIndex.MergeConflict {
    System.out.println("Saving index...");
    for (;;) {
      PreconditionFailed failure;
      try (InputStream bytes = transformer.followedBy(new Encryption(newPassword)).apply(indexFormat.serialize(index))) {
        tagForLastIndexLoad = indexStore.write(tagForLastIndexLoad, bytes);
        return;
      } catch (PreconditionFailed exn) {
        failure = exn;
      }

      System.out.println("Checkpoint failed due to " + failure);
      System.out.println("This probably happened because another process modified the");
      System.out.println("checkpoint while this process was running.  This process will");
      System.out.println("reload the checkpoint, merge its progress, and try again...");
      var latest = readLatestFromIndexStore(currentPassword);
      index = index.merge(latest.getSnd());
      tagForLastIndexLoad = latest.getFst();
    }
  }

}
