package cal.bkup.impls;

import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;

import java.io.IOException;

public class DummyTarget implements BackupTarget {

  private final BackupTarget wrapped;

  public DummyTarget(BackupTarget wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Id name() {
    return wrapped.name();
  }

  @Override
  public void backup(Resource r, IOConsumer<Id> k) throws IOException {
    k.accept(null);
  }

  @Override
  public void close() throws Exception {
    wrapped.close();
  }

}
