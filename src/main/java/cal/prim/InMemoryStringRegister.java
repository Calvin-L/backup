package cal.prim;

import java.util.Objects;

public class InMemoryStringRegister implements StringRegister {

  private String value = "";

  @Override
  public synchronized String read() {
    return value;
  }

  @Override
  public synchronized void write(String expectedValue, String newValue) throws PreconditionFailed {
    Objects.requireNonNull(newValue);
    if (!value.equals(expectedValue)) {
      throw new PreconditionFailed("expected " + expectedValue + " but got " + newValue);
    }
    value = newValue;
  }

}
