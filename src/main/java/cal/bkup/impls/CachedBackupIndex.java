package cal.bkup.impls;

import cal.bkup.types.BackupReport;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SystemId;
import cal.prim.MalformedDataException;
import cal.prim.NoValue;
import cal.prim.PreconditionFailed;
import cal.prim.QuietAutoCloseable;
import cal.prim.fs.HardLink;
import cal.prim.fs.SymLink;
import cal.prim.storage.ConsistentBlob;
import cal.prim.storage.ConsistentBlob.Tag;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.Encryption;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Provides efficient read/write access to a shared {@link BackupIndex} stored in a {@link ConsistentBlob}.
 * Helper type for {@link BackerUpper}.
 *
 * <p>This class has a great many responsibilities, including:
 * <ul>
 *   <li>Caching the index from the remote store</li>
 *   <li>Serializing/deserializing the index</li>
 *   <li>Encrypting/decrypting the index</li>
 * </ul>
 */
class CachedBackupIndex {

  private final ConsistentBlob indexStore;
  private final IndexFormat indexFormat;
  private final BlobTransformer transformer;


  // Cached data
  private @Nullable LoadedIndex indexAndTag = null;

  private record LoadedIndex(BackupIndex index, Tag tag, String password) {
  }

  public CachedBackupIndex(ConsistentBlob indexStore, IndexFormat indexFormat, BlobTransformer transformer) {
    this.indexStore = indexStore;
    this.indexFormat = indexFormat;
    this.transformer = transformer;
  }

  public BackupIndex getIndex(String password) throws IOException {
    return getIndexAndTag(password).index();
  }

  private LoadedIndex getIndexAndTag(String password) throws IOException {
    if (indexAndTag == null) {
      indexAndTag = readLatestFromIndexStore(password);
    }
    return indexAndTag;
  }

  public InputStream readRawIndex(String password) throws IOException, NoValue, ConsistentBlob.TagExpired {
    var rawStream = indexStore.read(indexStore.head());
    try {
      return transformer.followedBy(new Encryption(password)).unApply(rawStream);
    } catch (Exception e) {
      try {
        rawStream.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
  }

  private LoadedIndex readLatestFromIndexStore(String password) throws IOException {
    for (;;) {
      var tag = indexStore.head();
      BackupIndex index;
      System.out.println("Reading index " + tag + "...");
      try (InputStream raw = indexStore.read(tag);
           InputStream in = transformer.followedBy(new Encryption(password)).unApply(raw)) {
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
      return new LoadedIndex(index, tag, password);
    }
  }

  public void cleanup(boolean forReal) throws IOException {
    indexStore.cleanup(forReal);
  }

  public Transaction beginTransaction(String password) throws IOException {
    var indexAndTag = getIndexAndTag(password);
    return new Transaction(this, indexAndTag.index(), indexAndTag.tag(), password);
  }

  /**
   * Commit the contents of {@link #indexAndTag} to {@link #indexStore}.
   * If another process has modified the index stored in {@link #indexStore},
   * this method will download it, merge the other process's
   * modifications with this one, and try again.
   *
   * @param index the index to save
   * @param tag the expected tag in {@link #indexStore}
   * @param currentPassword the old password, used if the index needs to be downloaded
   * @param newPassword the password to encrypt the index
   * @param onConflict what to do if another process has modified the index since it was last read
   * @throws IOException
   * @throws cal.bkup.impls.BackupIndex.MergeConflict if another process modified the index, but the changes
   *         cannot be merged with the changes made by this process.  If this happens, the current {@link #indexAndTag}
   *         is replaced by the current values.
   */
  private LoadedIndex saveIndex(
      BackupIndex index,
      Tag tag,
      String currentPassword,
      String newPassword,
      ConflictBehavior onConflict)
      throws IOException, BackupIndex.MergeConflict {
    System.out.println("Saving index...");
    index.checkIntegrity();
    for (;;) {
      PreconditionFailed failure;
      try (InputStream raw = indexFormat.serialize(index);
           InputStream bytes = transformer.followedBy(new Encryption(newPassword)).apply(raw)) {
        var newTag = indexStore.write(tag, bytes);
        indexAndTag = new LoadedIndex(index, newTag, newPassword);
        return indexAndTag;
      } catch (PreconditionFailed exn) {
        failure = exn;
      }

      if (onConflict == ConflictBehavior.TRY_MERGE) {
        System.out.println("Checkpoint failed due to " + failure);
        System.out.println("This probably happened because another process modified the");
        System.out.println("checkpoint while this process was running.  This process will");
        System.out.println("reload the checkpoint, merge its progress, and try again...");
        var latest = readLatestFromIndexStore(currentPassword);
        indexAndTag = latest;
        index = index.merge(latest.index());
        tag = latest.tag();
      } else {
        // clear cached data
        indexAndTag = null;
        throw new BackupIndex.MergeConflict("The index could not be saved due to concurrent modification");
      }
    }
  }

  private enum ConflictBehavior {
    TRY_MERGE,
    ALWAYS_FAIL,
  }

  static final class Transaction implements QuietAutoCloseable {

    private final CachedBackupIndex parent;

    private BackupIndex index;

    private Tag expectedTag;

    private String password;

    private ConflictBehavior conflictBehavior;

    private boolean open;

    public Transaction(CachedBackupIndex parent, BackupIndex index, Tag expectedTag, String password) {
      this.parent = parent;
      this.index = new BackupIndex(index);
      this.expectedTag = expectedTag;
      this.password = password;
      this.conflictBehavior = ConflictBehavior.TRY_MERGE;
      this.open = true;
      this.index.checkIntegrity();
    }

    public Set<SystemId> knownSystems() {
      return index.knownSystems();
    }

    public List<BackupIndex.BackupMetadata> knownBackups(SystemId system) {
      return index.knownBackups(system);
    }

    public Set<Path> knownPaths(SystemId system) {
      return index.knownPaths(system);
    }

    public Stream<Sha256AndSize> listBlobs() {
      return index.listBlobs();
    }

    public @Nullable BackupReport lookupBlob(Sha256AndSize content) {
      return index.lookupBlob(content);
    }

    public BackupIndex.@Nullable Revision mostRecentRevision(SystemId system, Path path) {
      return index.mostRecentRevision(system, path);
    }

    public List<BackupIndex.Revision> getInfo(SystemId system, Path path) {
      return index.getInfo(system, path);
    }

    public BackupIndex.Revision resolveHardLinkTarget(SystemId system, BackupIndex.HardLinkRev hardLink) {
      return index.resolveHardLinkTarget(system, hardLink);
    }

    public BackupIndex.BackupMetadata startBackup(SystemId system, Instant now) {
      return index.startBackup(system, now);
    }

    public void finishBackup(SystemId system, BackupIndex.BackupMetadata info, Instant now) {
      index.finishBackup(system, info, now);
    }

    public void appendRevision(SystemId system, BackupIndex.BackupMetadata backup, Path path, Instant modTime, Sha256AndSize contentSummary) {
      index.appendRevision(system, backup, path, modTime, contentSummary);
    }

    public void appendRevision(SystemId system, BackupIndex.BackupMetadata backup, Path path, HardLink link) {
      index.appendRevision(system, backup, path, link);
    }

    public void appendRevision(SystemId system, BackupIndex.BackupMetadata backup, Path path, SymLink link) {
      index.appendRevision(system, backup, path, link);
    }

    public void appendTombstone(SystemId system, BackupIndex.BackupMetadata backup, Path path) {
      index.appendTombstone(system, backup, path);
    }

    public BackupReport addOrCanonicalizeBackedUpBlob(Sha256AndSize content, BackupReport backupReport) {
      return index.addOrCanonicalizeBackedUpBlob(content, backupReport);
    }

    public void forgetBlob(Sha256AndSize content) {
      index.forgetBlob(content);
    }

    public void forgetRevision(SystemId system, Path path, BackupIndex.Revision revision) {
      index.forgetRevision(system, path, revision);
    }

    public void forgetBackup(SystemId system, BackupIndex.BackupMetadata info) {
      index.forgetBackup(system, info);
    }

    public void forgetSystem(SystemId system) {
      index.forgetSystem(system);
    }

    public void bumpCleanupGeneration() {
      conflictBehavior = ConflictBehavior.ALWAYS_FAIL;
      index.bumpCleanupGeneration();
    }

    public void commit(String newPassword) throws BackupIndex.MergeConflict, IOException {
      if (!open) {
        throw new IllegalStateException("Transaction has already been closed");
      }
      var result = parent.saveIndex(index, expectedTag, password, newPassword, conflictBehavior);
      index = result.index;
      expectedTag = result.tag;
      password = result.password;
    }

    @Override
    public void close() {
      open = false;
    }
  }

}
