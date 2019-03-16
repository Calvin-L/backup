package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.CheckpointFormat;
import cal.bkup.types.HardLink;
import cal.bkup.types.Id;
import cal.bkup.types.IncorrectFormatException;
import cal.bkup.types.Resource;
import cal.bkup.types.ResourceInfo;
import cal.bkup.types.Sha256AndSize;
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
import java.util.stream.Stream;

public class SqliteCheckpoint2 implements Checkpoint, AutoCloseable {

  public static final CheckpointFormat FORMAT = new CheckpointFormat() {
    @Override
    public String name() {
      return "SQLite";
    }

    @Override
    public Checkpoint createEmpty() throws IOException {
      try {
        return new SqliteCheckpoint2();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Checkpoint tryRead(InputStream in) throws IOException, IncorrectFormatException {
      SqliteCheckpoint2 result;
      try {
        result = new SqliteCheckpoint2(in);
      } catch (SQLException e) {
        throw new IncorrectFormatException(e);
      }
      return result;
    }

    @Override
    public Checkpoint migrateFrom(Checkpoint oldCheckpoint) throws IOException {
      Checkpoint ck2 = createEmpty();
      oldCheckpoint.list().forEach(info -> {
        Sha256AndSize trueSummary;
        try {
          trueSummary = info.trueSummary();
        } catch (UnsupportedOperationException e) {
          System.err.println("Dropping mystery file " + info.path());
          return;
        }
        try {
          ck2.noteSuccessfulBackup(info.target(), new Resource() {
            @Override
            public Id system() {
              return info.system();
            }

            @Override
            public Path path() {
              return info.path();
            }

            @Override
            public Instant modTime() {
              return info.modTime();
            }

            @Override
            public InputStream open() throws IOException {
              return new FileInputStream(info.path().toFile());
            }

            @Override
            public long sizeEstimateInBytes() {
              throw new UnsupportedOperationException();
            }
          }, trueSummary, new BackupReport() {
            @Override
            public Id idAtTarget() {
              return info.idAtTarget();
            }

            @Override
            public long sizeAtTarget() {
              return info.sizeAtTarget();
            }
          });
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      });
      return ck2;
    }
  };

  private final Path file;
  private Connection conn;
  private PreparedStatement queryModTime;
  private PreparedStatement insertBlobRecord;
  private PreparedStatement insertFileRecord;
  private PreparedStatement queryAll;
  private PreparedStatement queryBlobByHash;
  private PreparedStatement insertSymLink;
  private PreparedStatement querySymLinksBySystem;
  private PreparedStatement queryHardLinksBySystem;
  private PreparedStatement insertHardLink;
  private PreparedStatement deleteFileRecord;
  private PreparedStatement deleteSymLink;
  private PreparedStatement deleteHardLink;

  private SqliteCheckpoint2() throws SQLException, IOException {
    file = Files.createTempFile("backup", "db");
    System.out.println("Created new checkpoint at " + file);
    reopen();
  }

  private SqliteCheckpoint2(InputStream in) throws IOException, SQLException, IncorrectFormatException {
    file = Files.createTempFile("backup", "db");
    System.out.println("Using SQLite file " + file);
    try (OutputStream out = new BufferedOutputStream(new FileOutputStream(file.toString()))) {
      Util.copyStream(in, out);
    }
    reopen();
    checkFormat();
  }

  private void checkFormat() throws IncorrectFormatException {
    try {
      list();
    } catch (IOException e) {
      throw new IncorrectFormatException(e.getMessage());
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
  public synchronized void noteSuccessfulBackup(Id backupTargetName, Resource r, Sha256AndSize contentSummary, BackupReport report) throws IOException {
    String sha256 = Util.sha256toString(contentSummary.sha256());
    try {
      queryBlobByHash.setString(1, backupTargetName.toString());
      queryBlobByHash.setString(2, sha256);
      queryBlobByHash.setLong(3, contentSummary.size());

      try (ResultSet rs = queryBlobByHash.executeQuery()) {
        if (!rs.next()) {
          insertBlobRecord.setString(1, sha256);
          insertBlobRecord.setLong(2, contentSummary.size());
          insertBlobRecord.setLong(3, report.sizeAtTarget());
          insertBlobRecord.setString(4, backupTargetName.toString());
          insertBlobRecord.setString(5, report.idAtTarget().toString());
          insertBlobRecord.executeUpdate();
        }
      }

      insertFileRecord.setString(1, r.system().toString());
      insertFileRecord.setString(2, r.path().toString());
      insertFileRecord.setLong(3, r.modTime().toEpochMilli());
      insertFileRecord.setString(4, sha256);
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

  @Override
  public void save(OutputStream out) throws IOException {
    try {
      synchronized (this) {
        close();
        try (InputStream reader = new FileInputStream(file.toString())) {
          Util.copyStream(reader, out);
        }
        reopen();
      }
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
          String sha = rs.getString("sha256");
          long size = rs.getLong("num_bytes");
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

            @Override
            public Sha256AndSize trueSummary() {
              return new Sha256AndSize() {
                @Override
                public byte[] sha256() {
                  return Util.stringToSha256(sha);
                }

                @Override
                public long size() {
                  return size;
                }
              };
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
  public synchronized BackupReport findBlob(BackupTarget target, byte[] sha256, long numBytes) throws IOException {
    String sha256string = Util.sha256toString(sha256);
    try {
      queryBlobByHash.setString(1, target.name().toString());
      queryBlobByHash.setString(2, sha256string);
      queryBlobByHash.setLong(3, numBytes);

      try (ResultSet rs = queryBlobByHash.executeQuery()) {
        if (rs.next()) {
          Id id = new Id(rs.getString("id"));
          long size = rs.getLong("num_bytes_at_target");
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
      }

    } catch (SQLException e) {
      throw new IOException(e);
    }

    return null;
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
  public synchronized void close() throws SQLException {
    conn.commit();
    if (insertFileRecord != null) { insertFileRecord.close(); insertFileRecord = null; }
    if (insertSymLink != null) { insertSymLink.close(); insertSymLink = null; }
    if (insertHardLink != null) { insertHardLink.close(); insertHardLink = null; }
    if (queryModTime != null) { queryModTime.close(); queryModTime = null; }
    if (queryAll != null) { queryAll.close(); queryAll = null; }
    if (queryBlobByHash != null) { queryBlobByHash.close(); queryBlobByHash = null; }
    if (queryHardLinksBySystem != null) { queryHardLinksBySystem.close(); queryHardLinksBySystem = null; }
    if (querySymLinksBySystem != null) { querySymLinksBySystem.close(); querySymLinksBySystem = null; }
    if (deleteFileRecord != null) { deleteFileRecord.close(); deleteFileRecord = null; }
    if (deleteSymLink != null) { deleteSymLink.close(); deleteSymLink = null; }
    if (deleteHardLink != null) { deleteHardLink.close(); deleteHardLink = null; }
    if (conn != null) { conn.close(); conn = null; }
  }

  private void reopen() throws SQLException {

    conn = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
    conn.setAutoCommit(false);

    try (Statement init = conn.createStatement()) {
      init.execute("CREATE TABLE IF NOT EXISTS blobs (sha256 TEXT, num_bytes INT, num_bytes_at_target INT, target TEXT, id TEXT)");
      init.execute("CREATE TABLE IF NOT EXISTS files (system TEXT, file TEXT, ms_since_unix_epoch INT, sha256 TEXT)");
      init.execute("CREATE TABLE IF NOT EXISTS symlinks (system TEXT, src TEXT, dst TEXT)");
      init.execute("CREATE TABLE IF NOT EXISTS hardlinks (system TEXT, src TEXT, dst TEXT)");

      // unique indexes
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS blob_hashes ON blobs (target, sha256, num_bytes)");
      init.execute("CREATE UNIQUE INDEX IF NOT EXISTS file_index ON files (system, file)");
    }
    conn.commit();

    queryModTime = conn.prepareStatement("SELECT ms_since_unix_epoch FROM files JOIN blobs ON files.sha256=blobs.sha256 WHERE system=? AND file=? AND target=? LIMIT 1");
    queryAll = conn.prepareStatement("SELECT system, file, target, id, ms_since_unix_epoch, files.sha256, num_bytes FROM files JOIN blobs ON files.sha256=blobs.sha256");
    querySymLinksBySystem = conn.prepareStatement("SELECT src, dst FROM symlinks WHERE system=?");
    queryHardLinksBySystem = conn.prepareStatement("SELECT src, dst FROM hardlinks WHERE system=?");
    queryBlobByHash = conn.prepareStatement("SELECT id, num_bytes_at_target FROM blobs WHERE target=? AND sha256=? AND num_bytes=? LIMIT 1");
    insertBlobRecord = conn.prepareStatement("INSERT INTO blobs (sha256, num_bytes, num_bytes_at_target, target, id) VALUES (?, ?, ?, ?, ?)");
    insertFileRecord = conn.prepareStatement("INSERT OR REPLACE INTO files (system, file, ms_since_unix_epoch, sha256) VALUES (?, ?, ?, ?)");
    insertSymLink = conn.prepareStatement("INSERT OR REPLACE INTO symlinks (system, src, dst) VALUES (?, ?, ?)");
    insertHardLink = conn.prepareStatement("INSERT OR REPLACE INTO hardlinks (system, src, dst) VALUES (?, ?, ?)");
    deleteFileRecord = conn.prepareStatement("DELETE FROM files WHERE system=? AND file=?");
    deleteSymLink = conn.prepareStatement("DELETE FROM symlinks WHERE system=? AND src=?");
    deleteHardLink = conn.prepareStatement("DELETE FROM hardlinks WHERE system=? AND src=?");

  }

}
