package cal.prim.concurrency;

import cal.prim.PreconditionFailed;

import java.util.Objects;

public class InMemoryStringRegister implements StringRegister {

  private String value = "";

  @Override
  public synchronized String read() {
    return value;
  }

  @Override
  public synchronized void write(String expectedValue, String newValue) throws PreconditionFailed {
    Objects.requireNonNull(expectedValue, "expected value may not be null");
    Objects.requireNonNull(newValue, "new value may not be null");

    if (!value.equals(expectedValue)) {
      throw new PreconditionFailed("expected " + expectedValue + " but value is currently " + value);
    }
    value = newValue;
  }

}
