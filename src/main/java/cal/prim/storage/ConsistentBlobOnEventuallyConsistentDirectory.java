package cal.prim.storage;

import cal.bkup.Util;
import cal.prim.NoValue;
import cal.prim.PreconditionFailed;
import cal.prim.concurrency.StringRegister;
import lombok.NonNull;
import lombok.Value;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConsistentBlobOnEventuallyConsistentDirectory implements ConsistentBlob {

  private static final Pattern NAME_PATTERN = Pattern.compile("backup-(\\d+)-.*");

  private final StringRegister clock;
  private final EventuallyConsistentDirectory directory;

  public ConsistentBlobOnEventuallyConsistentDirectory(StringRegister clock, EventuallyConsistentDirectory directory) throws IOException {
    if (clock.read().equals("")) {
      String name = freshName(0);
      directory.createOrReplace(name, new ByteArrayInputStream(new byte[0]));
      try {
        clock.write("", name);
      } catch (PreconditionFailed e) {
        throw new ConcurrentModificationException(e);
      }
    }
    this.clock = clock;
    this.directory = directory;
  }

  @Value
  private static class MyTag implements Tag {
    @NonNull String id;

    @Override
    public String toString() {
      return id;
    }
  }

  private MyTag upcast(Tag tag) {
    try {
      return (MyTag)tag;
    } catch (ClassCastException ignored) {
      throw new IllegalArgumentException("the given tag belongs to some other class");
    }
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
  public MyTag head() throws IOException {
    return new MyTag(clock.read());
  }

  @Override
  public InputStream read(Tag entry) throws IOException, NoValue, TagExpired {
    String id = upcast(entry).id;
    if (associatedClockValue(id) == 0L) {
      throw new NoValue();
    }
    for (;;) {
      try {
        return directory.open(id);
      } catch (NoSuchFileException ignored) {
        // What went wrong?
        //  (1) a new version of the object was created and the one
        //      with tag=`entry` is eligible for garbage collection, or
        //  (2) the underlying eventually-consistent directory has
        //      not made our object visible yet.
        // So, we shall read the current head.  If it isn't equal to the
        // given entry, then we definitely live in world (1) and we can
        // inform the client.  Otherwise, we live in world (2) and we
        // should retry.  (Of course, (1) might become true before we can
        // retry!)
        Tag head = head();
        if (!Objects.equals(head, entry)) {
          throw new TagExpired();
        }
      }
    }
  }

  private void asyncDelete(String name) {
    Util.async(() -> {
      try {
        directory.delete(name);
      } catch (IOException ignored) {
        // This is optional, so failure is ok.
        // The delete will happen later when the client calls cleanup().
      }
    });
  }

  @Override
  public Tag write(Tag expected, InputStream data) throws IOException, PreconditionFailed {
    String expectedId = upcast(expected).id;
    String currentName = clock.read();
    if (!currentName.equals(expectedId)) {
      throw new PreconditionFailed("expected "+ expectedId + " but got " + currentName);
    }

    long entryNumber = associatedClockValue(expectedId);
    String name = freshName(entryNumber + 1);
    directory.createOrReplace(name, data);
    try {
      clock.write(expectedId, name);
    } catch (PreconditionFailed e) {
      asyncDelete(name);
      throw e;
    }
    // It's technically safe to delete the old entry, but *just*in*case*
    // let's keep it around.  It will get clobbered by cleanup() later.
    // asyncDelete(expectedId);
    return new MyTag(name);
  }

  @Override
  public void cleanup(boolean forReal) throws IOException {
    String currentHead = head().id;
    final long cutoff = associatedClockValue(currentHead);

    // Find all the entries.
    Collection<String> toDelete = directory.list().collect(Collectors.toCollection(ArrayList::new));

    for (String name : toDelete) {
      final long clockValue;
      try {
        clockValue = associatedClockValue(name);
      } catch (IllegalArgumentException e) {
        System.err.println("WARNING: keeping object with malformed name " + name);
        continue;
      }

      // Notes:
      //  - The current head can NOT be deleted.
      //  - An object with a clock value < cutoff might have been a previous head, and can be
      //    deleted.
      //  - An object with a clock value = cutoff is either the current head or a write that
      //    produced (or will produce) a PreconditionFailed.  In the latter case, it can be
      //    deleted.
      //  - An object with a clock value > cutoff is a write that MAY succeed in the future, and
      //    can NOT be deleted.
      if (clockValue <= cutoff && !Objects.equals(name, currentHead)) {
        System.out.println("Deleting old blob " + name);
        if (forReal) {
          directory.delete(name);
        }
      }
    }

  }

}
