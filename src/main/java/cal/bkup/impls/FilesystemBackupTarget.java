package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;

public class FilesystemBackupTarget implements BackupTarget {

  private final Id id;
  private final Path root;

  private static String now() {
    return DateTimeFormatter.ISO_INSTANT.format(Instant.now());
  }

  public FilesystemBackupTarget(Path root) {
    id = new Id("file:" + root.toString());
    this.root = root.resolve(now());
  }

  @Override
  public Id name() {
    return id;
  }

  @Override
  public void backup(Resource r, IOConsumer<Id> k) throws IOException {
    Path dst = Paths.get(root.toString(), r.path().toAbsolutePath().toString());
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
    k.accept(new Id(dst.toString()));
  }

  @Override
  public void close() throws Exception {
  }

}
