package cal.bkup.types;

import java.io.IOException;

public interface Op<T> {
  Price cost();
  Price monthlyMaintenanceCost();
  long estimatedDataTransfer();
  T exec() throws IOException;
}
