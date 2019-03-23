package cal.prim;

public class NoValue extends Exception {
  public NoValue() {
  }

  public NoValue(String message) {
    super(message);
  }

  public NoValue(String message, Throwable cause) {
    super(message, cause);
  }

  public NoValue(Throwable cause) {
    super(cause);
  }

  public NoValue(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
