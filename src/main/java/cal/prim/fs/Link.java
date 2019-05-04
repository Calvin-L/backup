package cal.prim.fs;

import lombok.Value;
import lombok.experimental.NonFinal;

import java.nio.file.Path;

@Value
@NonFinal
public class Link {
  Path source;
  Path destination;

  /**
   * The Lombok-generated <code>equals</code> method returns false if
   * this returns false.  Here, it is overridden to prevent subclasses of
   * <code>Link</code> from equalling each other.  See also:
   * <a href="https://projectlombok.org/features/EqualsAndHashCode">the
   * Lombok documentation for <code>@EqualsAndHashCode</code></a>
   * @param other another object
   * @return true if <code>this</code> and <code>other</code> have exactly the same class
   */
  protected boolean canEqual(Object other) {
    return other != null && other.getClass() == getClass();
  }

}
