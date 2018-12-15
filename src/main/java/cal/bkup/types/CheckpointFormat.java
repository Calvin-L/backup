package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;

public interface CheckpointFormat {
  String name();
  Checkpoint createEmpty() throws IOException;
  Checkpoint tryRead(InputStream in) throws IOException, IncorrectFormatException;
  Checkpoint migrateFrom(Checkpoint result) throws IOException;
}
