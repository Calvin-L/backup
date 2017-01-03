package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
import cal.bkup.types.SimpleDirectory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqliteCheckpoint implements Checkpoint, AutoCloseable {

  private static final String EXTENSION = ".backupdb";

  private final SimpleDirectory dir;
  private final Path file;
  private volatile Long lastSave;
  private Connection conn;
  private PreparedStatement queryModTime;
  private PreparedStatement insertFileRecord;

  public SqliteCheckpoint(SimpleDirectory location) throws SQLException, IOException {
    dir = location;

    Pattern p = Pattern.compile("^(\\d+)" + Pattern.quote(EXTENSION) + "$");
    OptionalLong max = location.list()
        .map(p::matcher)
        .filter(Matcher::find)
        .mapToLong(m -> Long.parseLong(m.group(1)))
        .max();

    file = Files.createTempFile("backup", "db");
    if (max.isPresent()) {
      try (InputStream in = location.open(max.getAsLong() + EXTENSION);
          OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toString()))) {
        Util.copyStream(in, out);
        lastSave = max.getAsLong();
        System.out.println("Loaded checkpoint " + lastSave);
      } catch (IOException e) {
        System.out.println("Load failed [" + e + "]; creating new checkpoint");
        try (FileOutputStream out = new FileOutputStream(file.toString())) {
          FileChannel outChan = out.getChannel();
          outChan.truncate(0);
        }
        lastSave = null;
      }
    } else {
      System.out.println("Created new checkpoint");
      lastSave = null;
    }

    reopen();
  }

  @Override
  public synchronized Instant modTime(Resource r) {
    try {
      queryModTime.setString(1, r.path().toString());
      try (ResultSet rs = queryModTime.executeQuery()) {
        if (rs.next()) {
          long ms = rs.getLong("ms_since_unix_epoch");
          return Instant.ofEpochMilli(ms);
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public Instant lastSave() {
    Long ls = lastSave; // capture volatile var
    return ls != null ? Instant.ofEpochMilli(ls) : null;
  }

  @Override
  public synchronized void noteSuccessfulBackup(Resource r, BackupTarget target, Id asId) throws IOException {
    try {
      insertFileRecord.setString(1, r.path().toString());
      insertFileRecord.setLong(2, r.modTime().toEpochMilli());
      insertFileRecord.setString(3, target.name().toString());
      insertFileRecord.setString(4, asId.toString());
      insertFileRecord.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  protected synchronized void save(boolean reopen) throws IOException, SQLException {
    long timestamp = Instant.now().toEpochMilli();
    System.out.println("Saving checkpoint [" + timestamp + ']');
    conn.commit();
    if (queryModTime != null) { queryModTime.close(); queryModTime = null; }
    if (insertFileRecord != null) { insertFileRecord.close(); insertFileRecord = null; }
    if (conn != null) { conn.close(); conn = null; }
    try (InputStream reader = new FileInputStream(file.toString());
         OutputStream writer = dir.createOrReplace(timestamp + EXTENSION)) {
      Util.copyStream(reader, writer);
    }
    if (reopen) reopen();
    lastSave = timestamp;
    System.out.println("Checkpointed!");
  }

  @Override
  public void save() throws IOException {
    try {
      save(true);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException, SQLException {
    save(false);
  }

  private void reopen() throws IOException, SQLException {

    conn = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
    conn.setAutoCommit(false);

    try (Statement init = conn.createStatement()) {
      init.execute("CREATE TABLE IF NOT EXISTS files (file TEXT, ms_since_unix_epoch INT, target TEXT, id TEXT)");
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS file_idx ON files (file)");
    }
    conn.commit();

    queryModTime = conn.prepareStatement("SELECT ms_since_unix_epoch FROM files WHERE file=? LIMIT 1");
    insertFileRecord = conn.prepareStatement("INSERT INTO files (file, ms_since_unix_epoch, target, id) VALUES (?, ?, ?, ?)");

  }

}
