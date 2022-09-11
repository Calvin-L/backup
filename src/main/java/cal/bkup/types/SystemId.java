package cal.bkup.types;

/**
 * A unique identifier for a system.  A system is a computer with a filesystem.
 */
public record SystemId(String value) {
  @Override
  public String toString() {
    return value;
  }
}
