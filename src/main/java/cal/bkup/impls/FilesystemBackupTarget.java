package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Price;
import cal.bkup.types.Resource;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

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
  public Op<Id> backup(Resource r) throws IOException {
    long size = r.sizeEstimateInBytes();
    return new Op<Id>() {
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
        return size;
      }

      @Override
      public Id exec() throws IOException {
        Path dst = root.resolve(UUID.randomUUID().toString());
        if (Files.exists(dst)) {
          throw new IOException("refusing to overwrite " + dst);
        }
        Files.createDirectories(dst.getParent());
        FileOutputStream openFile = new FileOutputStream(dst.toString());
        try (BufferedOutputStream out = new BufferedOutputStream(openFile);
             InputStream in = r.open()) {
          Util.copyStream(in, out);
          out.flush();
          openFile.getFD().sync();
        }
        return new Id(dst.toString());
      }

      @Override
      public String toString() {
        return r.path().toString();
      }
    };
  }

  @Override
  public void close() throws Exception {
  }

}
