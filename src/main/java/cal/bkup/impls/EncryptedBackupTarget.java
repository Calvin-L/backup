package cal.bkup.impls;

import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.prim.Pair;
import cal.prim.Price;
import cal.bkup.types.ResourceInfo;
import cal.prim.transforms.DecryptedInputStream;
import cal.prim.transforms.EncryptedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.stream.Stream;

public class EncryptedBackupTarget implements BackupTarget {
  private final BackupTarget backupTarget;
  private final String password;

  public EncryptedBackupTarget(BackupTarget backupTarget, String password) {
    this.backupTarget = backupTarget;
    this.password = password;
  }

  @Override
  public Id name() {
    return backupTarget.name();
  }

  @Override
  public BackupReport backup(InputStream data, long estimatedByteCount) throws IOException {
    return backupTarget.backup(new EncryptedInputStream(data, password), estimatedByteCount);
  }

  /**
   * Estimate how many bytes it will take to store an encrypted version of some data.
   * @param originalSize - the number of bytes in the data
   * @return roughly how many bytes the encrypted version of the data will occupy
   */
  private long encryptedSizeEstimate(long originalSize) {
    // Empirically, the encryption seems to add ~130-160 bytes of overhead.
    return originalSize + 160;
  }

  @Override
  public Price estimatedCostOfDataTransfer(long resourceSizeInBytes) {
    return backupTarget.estimatedCostOfDataTransfer(encryptedSizeEstimate(resourceSizeInBytes));
  }

  @Override
  public Price estimatedMonthlyMaintenanceCost(long resourceSizeInBytes) {
    return backupTarget.estimatedMonthlyMaintenanceCost(encryptedSizeEstimate(resourceSizeInBytes));
  }

  @Override
  public Stream<BackedUpResourceInfo> list() throws IOException {
    return backupTarget.list();
  }

  @Override
  public Op<Void> delete(BackedUpResourceInfo id) throws IOException {
    return backupTarget.delete(id);
  }

  @Override
  public void close() throws Exception {
    backupTarget.close();
  }

  @Override
  public Op<Void> fetch(Collection<ResourceInfo> infos, IOConsumer<Pair<ResourceInfo, InputStream>> callback) throws IOException {
    return backupTarget.fetch(infos, res -> {
      callback.accept(new Pair<>(res.fst, new DecryptedInputStream(res.snd, password)));
    });
  }
}
