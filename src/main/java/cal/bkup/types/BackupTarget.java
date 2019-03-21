package cal.bkup.types;

import cal.prim.Price;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.stream.Stream;

public interface BackupTarget extends AutoCloseable {
  Id name();
  BackupReport backup(InputStream data, long estimatedByteCount) throws IOException;
  Price estimatedCostOfDataTransfer(long resourceSizeInBytes);
  Price estimatedMonthlyMaintenanceCost(long resourceSizeInBytes);
  Stream<BackedUpResourceInfo> list() throws IOException;
  Op<Void> delete(BackedUpResourceInfo obj) throws IOException;

  /**
   * Fetch some backed-up files.
   * <p><em>NOTE: The callback is responsible for closing the input stream.</em></p>
   * @param infos the files to fetch
   * @param callback a callback to execute for each one
   * @return an operation that fetches the files and executes the callback in the order they become available
   * @throws IOException if something goes wrong
   */
  Op<Void> fetch(Collection<ResourceInfo> infos, IOConsumer<Pair<ResourceInfo, InputStream>> callback) throws IOException;
}
