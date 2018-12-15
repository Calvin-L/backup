package cal.bkup.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.OptionalLong;
import java.util.stream.LongStream;

public interface CheckpointSequence {
  LongStream checkpointIDs() throws IOException;
  default OptionalLong mostRecentCheckpointID() throws IOException {
    return checkpointIDs().max();
  }
  InputStream read(long checkpointNumber) throws IOException;
  OutputStream write(long checkpointNumber) throws IOException;
}
