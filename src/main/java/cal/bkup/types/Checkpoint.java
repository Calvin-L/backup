package cal.bkup.types;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.stream.Stream;

public interface Checkpoint extends AutoCloseable {

  /**
   * @param r the resource to check
   * @return last modification time of <code>r</code>, or <code>null</code> if <code>r</code> is not backed up
   */
  @Nullable Instant modTime(Resource r, BackupTarget target);

  void noteSuccessfulBackup(Id backupTargetName, Resource r, Sha256AndSize contentSummary, BackupReport report) throws IOException;
  void noteSymLink(Id system, SymLink link) throws IOException;
  void noteHardLink(Id systemId, HardLink hardlink) throws IOException;

  void forgetBackup(Id system, Path p) throws IOException;
  void forgetSymLink(Id system, Path p) throws IOException;
  void forgetHardLink(Id system, Path p) throws IOException;

  void save(OutputStream out) throws IOException;

  Stream<ResourceInfo> list() throws IOException;
  @Nullable BackupReport findBlob(BackupTarget target, byte[] sha256, long numBytes) throws IOException;
  Stream<SymLink> symlinks(Id system) throws IOException;
  Stream<HardLink> hardlinks(Id system) throws IOException;

  @Override
  default void close() throws Exception {
  }

}
