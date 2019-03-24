package cal.bkup.types;

import cal.bkup.impls.BackupIndex;

import java.io.IOException;
import java.io.InputStream;

public interface IndexFormat {
  BackupIndex load(InputStream data) throws IOException;
  InputStream serialize(BackupIndex index) throws IOException;
}
