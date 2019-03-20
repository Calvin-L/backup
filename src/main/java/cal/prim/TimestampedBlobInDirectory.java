package cal.prim;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.LongStream;

public class TimestampedBlobInDirectory implements ConsistentBlob {
  private static final String EXTENSION = ".backupdb";
  private static final Pattern PATTERN = Pattern.compile("^(\\d+)" + Pattern.quote(EXTENSION) + "$");

  private final EventuallyConsistentDirectory location;

  public TimestampedBlobInDirectory(EventuallyConsistentDirectory location) {
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

  private LongStream checkpointIDs() throws IOException {
    return location.list()
            .map(this::parseNameAsSequenceNumber)
            .filter(OptionalLong::isPresent)
            .mapToLong(OptionalLong::getAsLong);
  }

  private static class MyTag implements Tag {
    final long timestamp;

    public MyTag(long timestamp) {
      this.timestamp = timestamp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      MyTag myTag = (MyTag) o;
      return timestamp == myTag.timestamp;
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp);
    }

    @Override
    public String toString() {
      return "MyTag{timestamp=" + timestamp + '}';
    }
  }

  @Override
  public Tag head() throws IOException {
    return new MyTag(checkpointIDs().max().orElse(-1));
  }

  @Override
  public InputStream read(Tag tag) throws IOException {
    long checkpointNumber = ((MyTag)tag).timestamp;
    return location.open(sequenceNumberToName(checkpointNumber));
  }

  @Override
  public Tag write(Tag tag, InputStream data) {
    throw new UnsupportedOperationException("this old class never really met its specification anyway");
  }

}
