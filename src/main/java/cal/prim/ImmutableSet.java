package cal.prim;

import javax.annotation.Nonnull;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

public class ImmutableSet<T> extends AbstractSet<T> {

  T[] data;

  @SuppressWarnings("unchecked")
  public ImmutableSet(Set<T> elements) {
    data = (T[])elements.toArray();
  }

  @Override
  public @Nonnull Iterator<T> iterator() {
    return Arrays.asList(data).iterator();
  }

  @Override
  public int size() {
    return data.length;
  }

}
