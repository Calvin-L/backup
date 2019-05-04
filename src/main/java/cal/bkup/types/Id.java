package cal.bkup.types;

import lombok.Value;

@Value
public class Id {
  String value;

  @Override
  public String toString() {
    return value;
  }
}
