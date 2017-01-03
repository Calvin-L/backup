package cal.bkup.types;

import java.io.IOException;
import java.util.function.Consumer;

public interface BackupTarget extends AutoCloseable {

  Id name();
  void backup(Resource r, IOConsumer<Id> k) throws IOException;

}
