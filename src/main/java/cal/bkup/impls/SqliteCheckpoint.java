package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
import cal.bkup.types.ResourceInfo;
import cal.bkup.types.SimpleDirectory;
import cal.bkup.types.SymLink;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SqliteCheckpoint implements Checkpoint, AutoCloseable {

  private static final String EXTENSION = ".backupdb";

  private final SimpleDirectory dir;
  private final Path file;
  private volatile Long lastSave;
  private Connection conn;
  private PreparedStatement queryModTime;
  private PreparedStatement insertFileRecord;
  private PreparedStatement queryAll;
  private PreparedStatement insertSymLink;
  private PreparedStatement queryAllSymlinks;

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
        Files.deleteIfExists(file);
        lastSave = null;
      }
    } else {
      System.out.println("Created new checkpoint");
      lastSave = null;
    }

    try {
      reopen();
    } catch (Exception e) {
      System.out.println("Load failed [" + e + "]; creating new checkpoint");
      Files.deleteIfExists(file);
      reopen();
      lastSave = null;
    }
  }

  @Override
  public synchronized Instant modTime(Resource r, BackupTarget target) {
    try {
      queryModTime.setString(1, r.system().toString());
      queryModTime.setString(2, r.path().toString());
      queryModTime.setString(3, target.name().toString());
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
      insertFileRecord.setString(1, r.system().toString());
      insertFileRecord.setString(2, r.path().toString());
      insertFileRecord.setLong(3, r.modTime().toEpochMilli());
      insertFileRecord.setString(4, target.name().toString());
      insertFileRecord.setString(5, asId.toString());
      insertFileRecord.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void noteSymLink(Id system, SymLink link) throws IOException {
    try {
      insertSymLink.setString(1, system.toString());
      insertSymLink.setString(2, link.src().toString());
      insertSymLink.setString(3, link.dst().toString());
      insertSymLink.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  protected synchronized void save(boolean reopen) throws IOException, SQLException {
    long timestamp = Instant.now().toEpochMilli();
    System.out.println("Saving checkpoint [" + timestamp + ']');
    close();
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
  public Stream<ResourceInfo> list() throws IOException {
    List<ResourceInfo> res = new ArrayList<>();

    try {
      try (ResultSet rs = queryAll.executeQuery()) {
        while (rs.next()) {
          Id sys = new Id(rs.getString("system"));
          Path file = Paths.get(rs.getString("file"));
          Id target = new Id(rs.getString("target"));
          Id id = new Id(rs.getString("id"));
          long ms = rs.getLong("ms_since_unix_epoch");
          Instant time = Instant.ofEpochMilli(ms);
          res.add(new ResourceInfo() {
            @Override
            public Id system() {
              return sys;
            }

            @Override
            public Path path() {
              return file;
            }

            @Override
            public Instant modTime() {
              return time;
            }

            @Override
            public Id target() {
              return target;
            }

            @Override
            public Id idAtTarget() {
              return id;
            }
          });
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }

    return res.stream();
  }

  @Override
  public Stream<SymLink> symlinks() throws IOException {
    List<SymLink> res = new ArrayList<>();

    try {
      try (ResultSet rs = queryAllSymlinks.executeQuery()) {
        while (rs.next()) {
          Path src = Paths.get(rs.getString("src"));
          Path dst = Paths.get(rs.getString("dst"));
          res.add(new SymLink() {
            @Override
            public Path src() {
              return src;
            }

            @Override
            public Path dst() {
              return dst;
            }
          });
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }

    return res.stream();
  }

  @Override
  public void close() throws IOException, SQLException {
    conn.commit();
    if (queryModTime != null) { queryModTime.close(); queryModTime = null; }
    if (insertFileRecord != null) { insertFileRecord.close(); insertFileRecord = null; }
    if (conn != null) { conn.close(); conn = null; }
  }

  private void reopen() throws IOException, SQLException {

    conn = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
    conn.setAutoCommit(false);

    try (Statement init = conn.createStatement()) {
      init.execute("CREATE TABLE IF NOT EXISTS files (system TEXT, file TEXT, ms_since_unix_epoch INT, target TEXT, id TEXT)");
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS file_idx ON files (system, file, target)");
      init.execute("CREATE TABLE IF NOT EXISTS symlinks (system TEXT, src TEXT, dst TEXT)");
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS symlink_idx ON symlinks (system, src)");
    }
    conn.commit();

    queryModTime = conn.prepareStatement("SELECT ms_since_unix_epoch FROM files WHERE system=? AND file=? AND target=? LIMIT 1");
    queryAll = conn.prepareStatement("SELECT system, file, target, id, ms_since_unix_epoch FROM files");
    queryAllSymlinks = conn.prepareStatement("SELECT src, dst FROM symlinks");
    insertFileRecord = conn.prepareStatement("INSERT OR REPLACE INTO files (system, file, ms_since_unix_epoch, target, id) VALUES (?, ?, ?, ?, ?)");
    insertSymLink = conn.prepareStatement("INSERT OR REPLACE INTO symlinks (system, src, dst) VALUES (?, ?, ?)");

  }

}
