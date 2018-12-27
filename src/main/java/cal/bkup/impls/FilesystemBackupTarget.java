package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Pair;
import cal.bkup.types.Price;
import cal.bkup.types.ResourceInfo;
import org.crashsafeio.AtomicDurableOutputStream;
import org.crashsafeio.DurableIOUtil;

import java.io.FileInputStream;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

public class FilesystemBackupTarget implements BackupTarget {

  private final Id id;
  private final Path root;

  public FilesystemBackupTarget(Path root) {
    id = new Id("file:" + root.toString());
    this.root = root;
  }

  @Override
  public Id name() {
    return id;
  }

  @Override
  public BackupReport backup(InputStream data, long estimatedByteCount) throws IOException {
    String uuid = UUID.randomUUID().toString();
    String prefix = uuid.substring(0, 2);
    Path dst = root.resolve(prefix).resolve(uuid);
    if (Files.exists(dst)) {
      throw new IOException("refusing to overwrite " + dst);
    }
    synchronized (this) {
      DurableIOUtil.createFolders(dst.getParent());
    }
    final long size;
    try (OutputStream out = new AtomicDurableOutputStream(dst)) {
      size = Util.copyStreamAndCaptureSize(data, out);
    }
    final Id id = new Id(dst.toString());
    return new BackupReport() {
      @Override
      public Id idAtTarget() {
        return id;
      }

      @Override
      public long sizeAtTarget() {
        return size;
      }
    };
  }

  @Override
  public Price estimatedCostOfDataTransfer(long resourceSizeInBytes) {
    return Price.ZERO;
  }

  @Override
  public Price estimatedMonthlyMaintenanceCost(long resourceSizeInBytes) {
    return Price.ZERO;
  }

  @Override
  public Stream<BackedUpResourceInfo> list() throws IOException {
    return Files.list(root)
        .map(Path::getFileName)
        .map(Object::toString)
        .flatMap(dir -> {
          try {
            return Files.list(root.resolve(dir))
                    .map(Path::getFileName)
                    .map(Object::toString)
                    .filter(f -> {
                      try {
                        UUID.fromString(f);
                        return true;
                      } catch (IllegalArgumentException ignored) {
                        return false;
                      }
                    })
                    .map(f -> new BackedUpResourceInfo() {
                      @Override
                      public Id idAtTarget() {
                        return new Id(f);
                      }

                      @Override
                      public long storedSizeInBytes() {
                        try {
                          return Files.size(root.resolve(f));
                        } catch (IOException e) {
                          return 0L;
                        }
                      }

                      @Override
                      public Instant backupTime() {
                        try {
                          return Files.getLastModifiedTime(root.resolve(f)).toInstant();
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    });
          } catch (IOException e) {
            throw new IOError(e);
          }
        });
  }

  @Override
  public Op<Void> delete(BackedUpResourceInfo id) throws IOException {
    String uuid = id.idAtTarget().toString();
    Path path = root.resolve(uuid.substring(0, 2)).resolve(uuid);
    return new FreeOp<Void>() {
      @Override
      public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
        Files.delete(path);
        return null;
      }

      @Override
      public String toString() {
        return "delete backed up file at " + path;
      }
    };
  }

  @Override
  public void close() {
  }

  @Override
  public Op<Void> fetch(Collection<ResourceInfo> infos, IOConsumer<Pair<ResourceInfo, InputStream>> callback) {
    return new FreeOp<Void>() {
      @Override
      public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
        for (ResourceInfo i : infos) {
          Path p = Paths.get(i.idAtTarget().toString());
          callback.accept(new Pair<>(i, new FileInputStream(p.toFile())));
        }
        return null;
      }

      @Override
      public String toString() {
        return "fetch " + infos.size() + " files";
      }
    };
  }

}
