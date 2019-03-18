package cal.prim;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConsistentBlobSequenceBackedByEventuallyConsistentDirectory implements ConsistentBlobSequence {

  private static final Pattern NAME_PATTERN = Pattern.compile("backup-(\\d+)-.*");

  private final StringRegister clock;
  private final EventuallyConsistentDirectory directory;

  public ConsistentBlobSequenceBackedByEventuallyConsistentDirectory(StringRegister clock, EventuallyConsistentDirectory directory) throws IOException {
    if (clock.read().equals("")) {
      try {
        String name = freshName(0);
        directory.createOrReplace(name, new ByteArrayInputStream(new byte[0]));
        clock.write("", name);
      } catch (PreconditionFailed e) {
        throw new ConcurrentModificationException(e);
      }
    }
    this.clock = clock;
    this.directory = directory;
  }

  private String freshName(long associatedClockValue) {
    return "backup-" + associatedClockValue + '-' + UUID.randomUUID().toString();
  }

  private long associatedClockValue(String name) {
    Matcher m = NAME_PATTERN.matcher(name);
    if (m.find()) {
      try {
        return Long.parseLong(m.group(1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("not a well-formed name: " + name, e);
      }
    }
    throw new IllegalArgumentException("not a well-formed name: " + name);
  }

  @Override
  public long head() throws IOException {
    return associatedClockValue(clock.read());
  }

  @Override
  public InputStream read(long entry) throws IOException {
    String name = clock.read();
    long value = associatedClockValue(name);
    if (value == entry) {
      return directory.open(name);
    }
    throw new IllegalArgumentException("Cannot open entry " + entry + "; the current head is " + value);
  }

  @Override
  public void writeSuccessor(long previousEntryNumber, long newEntryNumber, InputStream data) throws IOException, PreconditionFailed {
    if (newEntryNumber <= previousEntryNumber) {
      throw new IllegalArgumentException("new entry number must be larger than previous (given prev=" + previousEntryNumber + ", new=" + newEntryNumber + ')');
    }

    String currentName = clock.read();
    if (associatedClockValue(currentName) != previousEntryNumber) {
      throw new PreconditionFailed();
    }

    String name = freshName(newEntryNumber);
    directory.createOrReplace(name, data);
    clock.write(currentName, name);
  }

  @Override
  public void cleanupOldEntries(long current) throws IOException {

    final String currentName = clock.read();
    final long head = associatedClockValue(currentName);
    final long cutoff = Math.min(current, head);

    // Find all the entries.
    Collection<String> toDelete = directory.list().collect(Collectors.toCollection(ArrayList::new));
    toDelete.removeIf(name ->
            // Do not delete those whose clock values are > cutoff.
            associatedClockValue(name) > cutoff ||
            // Do not delete the current head.
            name.equals(currentName));

    // Do the deletion for those that remain.
    for (String name : toDelete) {
      directory.delete(name);
    }

  }

}
