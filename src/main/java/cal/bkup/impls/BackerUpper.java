package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.HardLink;
import cal.bkup.types.Id;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.RegularFile;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SymLink;
import cal.prim.ConsistentBlob;
import cal.prim.EventuallyConsistentBlobStore;
import cal.prim.NoValue;
import cal.prim.PreconditionFailed;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.Encryption;
import cal.prim.transforms.StatisticsCollectingInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
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

  // Cached data
  private BackupIndex index = null;
  private ConsistentBlob.Tag tagForLastIndexLoad = null;

  public BackerUpper(ConsistentBlob indexStore, IndexFormat indexFormat, EventuallyConsistentBlobStore blobStore, BlobTransformer transformer) {
    this.indexStore = indexStore;
    this.indexFormat = indexFormat;
    this.blobStore = blobStore;
    this.transformer = transformer;
  }

  public void backup(Id whatSystemIsThis, String passwordForIndex, String newPasswordForIndex, Stream<RegularFile> files, Stream<SymLink> symlinks, Stream<HardLink> hardlinks) throws IOException {
    String currentPassword = passwordForIndex;
    loadIndexIfMissing(currentPassword);

    Set<Path> inThisBackup = new HashSet<>();
    Iterator<RegularFile> it = files.iterator();
    Iterator<SymLink> symLinkIterator = symlinks.iterator();
    Iterator<HardLink> hardLinkIterator = hardlinks.iterator();

    for (;;) {
      try {
        Instant lastIndexSave = Instant.now();

        try {
          while (it.hasNext()) {
            RegularFile f = it.next();
            inThisBackup.add(f.path());
            BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, f.path());
            if (latest != null && latest.type == BackupIndex.FileType.REGULAR_FILE && latest.modTime.equals(f.modTime())) {
              // skip based on modification time
              continue;
            }
            uploadAndAddToIndex(index, whatSystemIsThis, f);
            Instant now = Instant.now();
            if (Util.ge(now, lastIndexSave.plus(Duration.ofMinutes(5)))) {
              saveIndex(newPasswordForIndex);
              currentPassword = newPasswordForIndex;
              lastIndexSave = now;
            }
          }

          while (symLinkIterator.hasNext()) {
            SymLink link = symLinkIterator.next();
            inThisBackup.add(link.src());
            BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.src());
            if (latest != null && latest.type == BackupIndex.FileType.SOFT_LINK && latest.linkTarget.equals(link.dst())) {
              continue;
            }
            System.out.println("Adding symlink " + link.src() + " --> " + link.dst());
            index.appendRevision(whatSystemIsThis, link.src(), link);
          }

          while (hardLinkIterator.hasNext()) {
            HardLink link = hardLinkIterator.next();
            inThisBackup.add(link.src());
            BackupIndex.Revision latest = index.mostRecentRevision(whatSystemIsThis, link.src());
            if (latest != null && latest.type == BackupIndex.FileType.HARD_LINK && latest.linkTarget.equals(link.dst())) {
              continue;
            }
            System.out.println("Adding hard link " + link.src() + " --> " + link.dst());
            index.appendRevision(whatSystemIsThis, link.src(), link);
          }

          for (Path p : index.knownPaths(whatSystemIsThis)) {
            if (!inThisBackup.contains(p)) {
              index.appendTombstone(whatSystemIsThis, p);
            }
          }
        } finally {
          saveIndex(newPasswordForIndex);
        }

        return;
      } catch (PreconditionFailed ignored) {
        System.out.println("Checkpoint failed due to " + ignored);
        System.out.println("This probably happened because another process modified the");
        System.out.println("checkpoint while this process was running.  This process will");
        System.out.println("reload the checkpoint, merge its progress, and try again...");

        BackupIndex myIndex = index;
        forceReloadIndex(currentPassword);
        index = index.merge(myIndex);

        // No need to reset the iterators!
        // The files that were backed up have been merged into the index.
      }
    }
  }

  public static class BackedUpThing {
    private final Id system;
    private final Path path;
    private final BackupIndex.Revision latestRevision;

    public BackedUpThing(Id system, Path path, BackupIndex.Revision latestRevision) {
      this.system = system;
      this.path = path;
      this.latestRevision = latestRevision;
    }

    public Id system() {
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

  void restore(String indexPassword, Stream<Path> files, Path outputDir) throws IOException {
    loadIndexIfMissing(indexPassword);
    throw new UnsupportedOperationException();
  }

  public void cleanup() throws IOException {
    indexStore.cleanup();
    // TODO: prune old data from BackupIndex
    // TODO: when can we delete things in the blob store?
    throw new UnsupportedOperationException();
  }

  // -------------------------------------------------------------------------
  // Helpers for doing backups

  private void uploadAndAddToIndex(BackupIndex index, Id systemId, RegularFile f) throws IOException {
    // (1) Get the modification time.
    // This has to happen before computing the checksum.  Otherwise,
    // we might miss a new revision of the file on a future backup.
    // This could happen if the file changes after computing the
    // checksum but before reading the modification time.
    Instant modTime = f.modTime();

    // (2) Checksum the path.
    Sha256AndSize summary;
    System.out.println("Computing checksum of " + f.path() + "...");
    try (InputStream in = Util.buffered(f.open())) {
      summary = Util.summarize(in, stream -> { });
    }

    // (3) Is this file known to the index?  Then no problem!
    BackupReport report = index.lookupBlob(summary);

    // (4) Not known?  Upload it!
    if (report == null) {
      System.out.println("Uploading " + f.path() + " (" + summary.size() + " bytes)...");
      String key = Util.randomPassword();
      Consumer<StatisticsCollectingInputStream> reportProgress = s -> {
      };
      Id identifier;
      long sizeAtTarget;
      StatisticsCollectingInputStream in = new StatisticsCollectingInputStream(Util.buffered(f.open()), reportProgress);
      try (InputStream transformed = transformer.followedBy(new Encryption(key)).apply(in)) {
        EventuallyConsistentBlobStore.PutResult result = blobStore.createOrReplace(transformed);
        identifier = new Id(result.identifier());
        sizeAtTarget = result.bytesStored();
      }
      System.out.println(" *** uploaded as " + identifier + " (" + sizeAtTarget + " bytes)");
      assert in.isClosed();
      long size = in.getBytesRead();
      byte[] sha256 = in.getSha256Digest();
      report = new BackupReport() {
        @Override
        public Id idAtTarget() {
          return identifier;
        }

        @Override
        public long sizeAtTarget() {
          return sizeAtTarget;
        }
      };
      summary = new Sha256AndSize(sha256, size);
      index.addBackedUpBlob(summary, key, report);
    } else {
      System.out.println(" *** new upload is not necessary");
    }

    // Record the latest details.
    System.out.println("Adding regular file " + f.path());
    index.appendRevision(systemId, f.path(), modTime, summary);
  }

  // -------------------------------------------------------------------------
  // Helpers for managing the index

  private void forceReloadIndex(String password) throws IOException {
    tagForLastIndexLoad = indexStore.head();
    try (InputStream in = transformer.followedBy(new Encryption(password)).unApply(indexStore.read(tagForLastIndexLoad))) {
      index = indexFormat.load(in);
    } catch (NoValue noValue) {
      System.out.println("No index was found; creating a new one");
      index = new BackupIndex();
    }
  }

  private void loadIndexIfMissing(String password) throws IOException {
    if (index == null) {
      forceReloadIndex(password);
    }
  }

  private void saveIndex(String password) throws IOException, PreconditionFailed {
    System.out.println("Saving index...");
    try (InputStream bytes = transformer.followedBy(new Encryption(password)).apply(indexFormat.serialize(index))) {
      tagForLastIndexLoad = indexStore.write(tagForLastIndexLoad, bytes);
    }
  }

}
