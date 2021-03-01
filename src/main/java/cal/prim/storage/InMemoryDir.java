package cal.prim.storage;

import cal.bkup.Util;
import cal.prim.Pair;
import com.google.common.collect.ImmutableList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An in-memory eventually-consistent directory with the weakest possible guarantees:
 * <ul>
 *   <li>{@link #list()} may return any subset of the entries that have ever existed,
 *       and they may be returned in any order</li>
 *   <li>{@link #open(String)} may return any past version of any object (including
 *       its initial "missing" version)</li>
 * </ul>
 *
 * These seemingly-useless guarantees are really the strongest we can rely on for
 * eventually-consistent cloud-backed data stores.  This class shines in tests, as
 * it may reveal surprising edge cases.
 */
public class InMemoryDir implements EventuallyConsistentDirectory {

  /** pending, unresolved calls to {@link #createOrReplace(String, InputStream)} */
  private final List<Pair<String, byte[]>> pendingWrites = new ArrayList<>();

  /** random number generator for maybe resolving pending calls */
  private final Random random = new Random(33);

  @Override
  public Stream<String> list() {
    List<String> legalEntries;
    synchronized (this) {
      legalEntries = pendingWrites.stream()
              .map(Pair::getFst)
              .distinct()
              .filter(name -> random.nextBoolean())
              .collect(Collectors.toList());
    }
    Collections.shuffle(legalEntries, random);
    return legalEntries.stream();
  }

  @Override
  public void createOrReplace(String name, InputStream s) throws IOException {
    byte[] bytes = Util.read(s);
    System.out.println("set dir[" + name + "] (size=" + bytes.length + ')');
    synchronized (this) {
      pendingWrites.add(new Pair<>(name, bytes));
    }
  }

  @Override
  public InputStream open(String name) throws NoSuchFileException {
    List<byte[]> legalEntries;
    synchronized (this) {
      legalEntries = pendingWrites.stream()
              .filter(entry -> entry.getFst().equals(name))
              .map(Pair::getSnd)
              .collect(Collectors.toList());
    }
    int index = random.nextInt(legalEntries.size() + 1);
    if (index >= legalEntries.size()) {
      throw new NoSuchFileException(name);
    }
    return new ByteArrayInputStream(legalEntries.get(index));
  }

  @Override
  public void delete(String name) {
    // There is no need to do anything here.  Both list() and open() can always
    // fail to see an object, whether or not it has ever been deleted.
  }

  /**
   * Get the list of pending object creations.
   *
   * <p>Note that this method is specific to this in-memory representation, and is
   * not generally available for eventually-consistent directories.  Its main
   * purpose is for testing.
   *
   * @return the list of pending writes
   */
  public synchronized List<Pair<String, byte[]>> getPendingWrites() {
    return ImmutableList.copyOf(pendingWrites);
  }

  public ConsistentInMemoryDir settle() {
    ConsistentInMemoryDir result = new ConsistentInMemoryDir();
    for (var entry : pendingWrites) {
      try {
        result.createOrReplace(entry.getFst(), new ByteArrayInputStream(entry.getSnd()));
      } catch (IOException e) {
        throw new IllegalStateException();
      }
    }
    return result;
  }

}
