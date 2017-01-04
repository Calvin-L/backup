package cal.bkup.impls;

import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
import cal.bkup.types.ResourceInfo;
import cal.bkup.types.SymLink;

import java.io.IOException;
import java.time.Instant;
import java.util.stream.Stream;

public class DummyCheckpoint implements Checkpoint {

  private final Checkpoint wrapped;

  public DummyCheckpoint(Checkpoint wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Instant modTime(Resource r, BackupTarget target) {
    return wrapped.modTime(r, target);
  }

  @Override
  public Instant lastSave() {
    return wrapped.lastSave();
  }

  @Override
  public void noteSuccessfulBackup(Resource r, BackupTarget target, Id asId) throws IOException {
    wrapped.noteSuccessfulBackup(r, target, asId);
  }

  @Override
  public void noteSymLink(Id system, SymLink link) throws IOException {
    wrapped.noteSymLink(system, link);
  }

  @Override
  public void save() {
    // nop
  }

  @Override
  public Stream<ResourceInfo> list() throws IOException {
    return wrapped.list();
  }

  @Override
  public Stream<SymLink> symlinks() throws IOException {
    return wrapped.symlinks();
  }

  @Override
  public void close() throws Exception {
    wrapped.close();
  }

}
