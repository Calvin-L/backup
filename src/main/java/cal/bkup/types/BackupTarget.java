package cal.bkup.types;

import java.io.IOException;
import java.util.stream.Stream;

public interface BackupTarget extends AutoCloseable {
  Id name();
  Op<Id> backup(Resource r) throws IOException;
  Stream<BackedUpResourceInfo> list() throws IOException;
  Op<Void> delete(BackedUpResourceInfo obj) throws IOException;
}
