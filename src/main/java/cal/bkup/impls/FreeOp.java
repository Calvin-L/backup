package cal.bkup.impls;

import cal.bkup.types.Op;
import cal.bkup.types.Price;

public abstract class FreeOp<T> implements Op<T> {
  @Override
  public Price cost() {
    return Price.ZERO;
  }

  @Override
  public Price monthlyMaintenanceCost() {
    return Price.ZERO;
  }

  @Override
  public long estimatedDataTransfer() {
    return 0;
  }
}
