package cal.bkup.types;

import cal.prim.Price;

public interface StorageCostModel {
  Price costToUploadBlob(long numBytes);
  Price monthlyStorageCostForBlob(long numBytes);
}
