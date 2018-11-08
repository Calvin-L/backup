package cal.bkup.impls;

import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Pair;
import cal.bkup.types.Resource;
import cal.bkup.types.ResourceInfo;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Instant;
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
  public Op<BackupReport> backup(Resource r) throws IOException {
    return backupTarget.backup(new Resource() {
      @Override
      public Id system() {
        return r.system();
      }

      @Override
      public Path path() {
        return r.path();
      }

      @Override
      public Instant modTime() throws IOException {
        return r.modTime();
      }

      @Override
      public InputStream open() throws IOException {
        try {
          return new EncryptedInputStream(r.open(), password);
        } catch (GeneralSecurityException e) {
          throw new IOException(e);
        }
      }

      @Override
      public long sizeEstimateInBytes() throws IOException {
        // TODO: can we do better?
        return r.sizeEstimateInBytes();
      }
    });
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
