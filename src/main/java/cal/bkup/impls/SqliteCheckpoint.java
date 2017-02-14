package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.HardLink;
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

  private SimpleDirectory dir;
  private final Path file;
  private volatile Long lastSave;
  private Connection conn;
  private PreparedStatement queryModTime;
  private PreparedStatement insertFileRecord;
  private PreparedStatement queryAll;
  private PreparedStatement insertSymLink;
  private PreparedStatement querySymLinksBySystem;
  private PreparedStatement queryHardLinksBySystem;
  private PreparedStatement insertHardLink;
  private PreparedStatement deleteFileRecord;
  private PreparedStatement deleteSymLink;
  private PreparedStatement deleteHardLink;

  public SqliteCheckpoint(SimpleDirectory location) throws SQLException, IOException {
    dir = location;

    Pattern p = Pattern.compile("^(\\d+)" + Pattern.quote(EXTENSION) + "$");
    OptionalLong max = location.list()
        .map(p::matcher)
        .filter(Matcher::find)
        .mapToLong(m -> Long.parseLong(m.group(1)))
        .max();

    file = Files.createTempFile("backup", "db");
    System.out.println("Using SQLite file " + file);
    if (max.isPresent()) {
      try (InputStream in = location.open(max.getAsLong() + EXTENSION);
          OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toString()))) {
        Util.copyStream(in, out);
      }
      lastSave = max.getAsLong();
      System.out.println("Loaded checkpoint " + lastSave);
    } else {
      lastSave = null;
      System.out.println("Created new checkpoint");
    }

    reopen();
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

  @Override
  public synchronized void noteHardLink(Id system, HardLink hardlink) throws IOException {
    try {
      insertHardLink.setString(1, system.toString());
      insertHardLink.setString(2, hardlink.src().toString());
      insertHardLink.setString(3, hardlink.dst().toString());
      insertHardLink.executeUpdate();
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
  public synchronized Stream<ResourceInfo> list() throws IOException {
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
            public long sizeAtTarget() {
              return 0; // TODO
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
  public synchronized Stream<SymLink> symlinks(Id system) throws IOException {
    List<SymLink> res = new ArrayList<>();

    try {
      querySymLinksBySystem.setString(1, system.toString());
      try (ResultSet rs = querySymLinksBySystem.executeQuery()) {
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
  public synchronized Stream<HardLink> hardlinks(Id system) throws IOException {
    List<HardLink> res = new ArrayList<>();

    try {
      queryHardLinksBySystem.setString(1, system.toString());
      try (ResultSet rs = queryHardLinksBySystem.executeQuery()) {
        while (rs.next()) {
          Path src = Paths.get(rs.getString("src"));
          Path dst = Paths.get(rs.getString("dst"));
          res.add(new HardLink() {
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
  public synchronized void forgetBackup(Id system, Path p) throws IOException {
    try {
      deleteFileRecord.setString(1, system.toString());
      deleteFileRecord.setString(2, p.toString());
      deleteFileRecord.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void forgetSymLink(Id system, Path p) throws IOException {
    try {
      deleteSymLink.setString(1, system.toString());
      deleteSymLink.setString(2, p.toString());
      deleteSymLink.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void forgetHardLink(Id system, Path p) throws IOException {
    try {
      deleteHardLink.setString(1, system.toString());
      deleteHardLink.setString(2, p.toString());
      deleteHardLink.executeUpdate();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  @Override
  public synchronized void close() throws IOException, SQLException {
    conn.commit();
    if (insertFileRecord != null) { insertFileRecord.close(); insertFileRecord = null; }
    if (insertSymLink != null) { insertSymLink.close(); insertSymLink = null; }
    if (insertHardLink != null) { insertHardLink.close(); insertHardLink = null; }
    if (queryModTime != null) { queryModTime.close(); queryModTime = null; }
    if (queryAll != null) { queryAll.close(); queryAll = null; }
    if (queryHardLinksBySystem != null) { queryHardLinksBySystem.close(); queryHardLinksBySystem = null; }
    if (querySymLinksBySystem != null) { querySymLinksBySystem.close(); querySymLinksBySystem = null; }
    if (deleteFileRecord != null) { deleteFileRecord.close(); deleteFileRecord = null; }
    if (deleteSymLink != null) { deleteSymLink.close(); deleteSymLink = null; }
    if (deleteHardLink != null) { deleteHardLink.close(); deleteHardLink = null; }
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
      init.execute("CREATE TABLE IF NOT EXISTS hardlinks (system TEXT, src TEXT, dst TEXT)");
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS hardlink_idx ON hardlinks (system, src)");
    }
    conn.commit();

    queryModTime = conn.prepareStatement("SELECT ms_since_unix_epoch FROM files WHERE system=? AND file=? AND target=? LIMIT 1");
    queryAll = conn.prepareStatement("SELECT system, file, target, id, ms_since_unix_epoch FROM files");
    querySymLinksBySystem = conn.prepareStatement("SELECT src, dst FROM symlinks WHERE system=?");
    queryHardLinksBySystem = conn.prepareStatement("SELECT src, dst FROM hardlinks WHERE system=?");
    insertFileRecord = conn.prepareStatement("INSERT OR REPLACE INTO files (system, file, ms_since_unix_epoch, target, id) VALUES (?, ?, ?, ?, ?)");
    insertSymLink = conn.prepareStatement("INSERT OR REPLACE INTO symlinks (system, src, dst) VALUES (?, ?, ?)");
    insertHardLink = conn.prepareStatement("INSERT OR REPLACE INTO hardlinks (system, src, dst) VALUES (?, ?, ?)");
    deleteFileRecord = conn.prepareStatement("DELETE FROM files WHERE system=? AND file=?");
    deleteSymLink = conn.prepareStatement("DELETE FROM symlinks WHERE system=? AND src=?");
    deleteHardLink = conn.prepareStatement("DELETE FROM hardlinks WHERE system=? AND src=?");

  }

  public void moveTo(SimpleDirectory dir) throws IOException, SQLException {
    this.dir = dir;
    save(true);
  }

}
