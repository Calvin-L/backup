package cal.prim;

public class PreconditionFailed extends Exception {
  public PreconditionFailed() {
  }

  public PreconditionFailed(String message) {
    super(message);
  }

  public PreconditionFailed(String message, Throwable cause) {
    super(message, cause);
  }

  public PreconditionFailed(Throwable cause) {
    super(cause);
  }

  public PreconditionFailed(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
