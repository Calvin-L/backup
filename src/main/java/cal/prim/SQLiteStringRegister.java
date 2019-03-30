package cal.prim;

import org.crashsafeio.DurableIOUtil;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * A <code>StringRegister</code> implementation atop a SQLite database in a local file.
 */
public class SQLiteStringRegister implements StringRegister {

  private final Connection conn;

  public SQLiteStringRegister(Path filename) throws SQLException, IOException {
    DurableIOUtil.createFolders(filename.getParent());

    conn = DriverManager.getConnection("jdbc:sqlite:" + filename.toAbsolutePath());
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);

    try (Statement stmt = conn.createStatement()) {
      stmt.executeUpdate("CREATE TABLE IF NOT EXISTS tbl (value TEXT)");
      stmt.executeUpdate("INSERT OR IGNORE INTO tbl (rowid, value) VALUES (0, \"\")");
    }
    conn.commit();
  }

  @Override
  public String read() throws IOException {
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet rs = stmt.executeQuery("SELECT value FROM tbl LIMIT 1")) {
        if (rs.next()) {
          return rs.getString(1);
        }
      }
    } catch (SQLException e) {
      throw new IOException(e);
    }
    return "";
  }

  @Override
  public void write(String expectedValue, String newValue) throws IOException, PreconditionFailed {
    Objects.requireNonNull(newValue);
    try (PreparedStatement stmt = conn.prepareStatement("UPDATE tbl SET value=? WHERE value=?")) {
      stmt.setString(1, newValue);
      stmt.setString(2, expectedValue);
      if (stmt.executeUpdate() != 1) {
        throw new PreconditionFailed();
      }
      conn.commit();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

}
