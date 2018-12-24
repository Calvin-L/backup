package cal.bkup.types;

import cal.bkup.impls.ProgressDisplay;

import java.io.IOException;

public interface Op<T> {
  Price cost();
  Price monthlyMaintenanceCost();
  long estimatedDataTransfer();
  T exec(ProgressDisplay.ProgressCallback callback) throws IOException;
}
