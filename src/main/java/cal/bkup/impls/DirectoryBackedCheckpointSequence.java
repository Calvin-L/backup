package cal.bkup.impls;

import cal.bkup.types.CheckpointSequence;
import cal.bkup.types.SimpleDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

public class DirectoryBackedCheckpointSequence implements CheckpointSequence {
  private static final String EXTENSION = ".backupdb";
  private static final Pattern PATTERN = Pattern.compile("^(\\d+)" + Pattern.quote(EXTENSION) + "$");

  private final SimpleDirectory location;

  public DirectoryBackedCheckpointSequence(SimpleDirectory location) {
    this.location = location;
  }

  private OptionalLong parseNameAsSequenceNumber(String name) {
    Matcher m = PATTERN.matcher(name);
    if (m.find()) {
      try {
        return OptionalLong.of(Long.parseLong(m.group(1)));
      } catch (NumberFormatException ignored) {
      }
    }
    return OptionalLong.empty();
  }

  private String sequenceNumberToName(long number) {
    return number + EXTENSION;
  }

  @Override
  public LongStream checkpointIDs() throws IOException {
    return location.list()
            .map(this::parseNameAsSequenceNumber)
            .filter(OptionalLong::isPresent)
            .mapToLong(OptionalLong::getAsLong);
  }

  @Override
  public InputStream read(long checkpointNumber) throws IOException {
    return location.open(sequenceNumberToName(checkpointNumber));
  }

  @Override
  public OutputStream write(long checkpointNumber) throws IOException {
    String name = sequenceNumberToName(checkpointNumber);
    OptionalLong doublecheck = parseNameAsSequenceNumber(name);
    assert doublecheck.isPresent();
    assert doublecheck.getAsLong() == checkpointNumber;
    return location.create(name);
  }
}
