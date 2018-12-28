package cal.bkup.impls;

import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Pair;
import cal.bkup.types.Price;
import cal.bkup.types.ResourceInfo;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
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
    try {
      return backupTarget.backup(new EncryptedInputStream(data, password), estimatedByteCount);
    } catch (GeneralSecurityException e) {
      throw new IOException(e);
    }
  }

  /**
   * Estimate how many bytes it will take to store an encrypted version of some data.
   * @param originalSize - the number of bytes in the data
   * @return roughly how many bytes the encrypted version of the data will occupy
   */
  private long encryptedSizeEstimate(long originalSize) {
    // TODO: is this right? can we do better?
    return originalSize;
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
      try {
        callback.accept(new Pair<>(res.fst, new DecryptedInputStream(res.snd, password)));
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
    });
  }
}
