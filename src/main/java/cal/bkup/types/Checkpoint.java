package cal.bkup.types;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

public interface Checkpoint extends AutoCloseable {

  /**
   * @param r the resource to check
   * @return last modification time of <code>r</code>, or <code>null</code> if <code>r</code> is not backed up
   */
  Instant modTime(Resource r, BackupTarget target);

  Instant lastSave();

  void noteSuccessfulBackup(Resource r, BackupTarget target, Id asId) throws IOException;
  void noteSymLink(Id system, SymLink link) throws IOException;
  void noteHardLink(Id systemId, HardLink hardlink) throws IOException;

  void save() throws IOException;

  Stream<ResourceInfo> list() throws IOException;
  Stream<SymLink> symlinks() throws IOException;
  Stream<HardLink> hardlinks() throws IOException;

  @Override
  default void close() throws Exception {
  }

}
