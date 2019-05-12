package cal.bkup.types;

import lombok.Value;

/**
 * A unique identifier for a system.  A system is a computer with a filesystem.
 */
@Value
public class SystemId {
  String value;

  @Override
  public String toString() {
    return value;
  }
}
