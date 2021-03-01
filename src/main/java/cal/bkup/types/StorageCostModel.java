package cal.bkup.types;

import cal.prim.Price;

import java.time.Duration;

public interface StorageCostModel {
  Price costToUploadBlob(long numBytes);
  Price costToDeleteBlob(long numBytes, Duration timeSinceUpload);
  Price monthlyStorageCostForBlob(long numBytes);
}
