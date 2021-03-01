package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.SystemId;
import cal.prim.MalformedDataException;
import cal.prim.fs.HardLink;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.prim.fs.SymLink;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JsonIndexFormatV03 implements IndexFormat {

  private static class JsonBackupInfo {
    public BigInteger startTimeAsMillisecondsSinceEpoch;
    public BigInteger endTimeAsMillisecondsSinceEpoch;
    public long backupNumber;
  }

  private static class JsonRevision {
    public long backupNumber;
    public BigInteger modTimeAsMillisecondsSinceEpoch;
    public String sha256;
    public long size;
    public String symLinkDst;
    public String hardLinkDst;
  }

  private static class JsonBlob {
    public String key;
    public String sha256;
    public long size;
    public String backupId;
    public long backupSize;
  }

  private static class JsonIndex {
    public Map<String, List<JsonBackupInfo>> history = new HashMap<>();
    public Map<String, Map<String, List<JsonRevision>>> files = new HashMap<>();
    public Set<JsonBlob> blobs = new HashSet<>();
    public BigInteger cleanupGeneration = BigInteger.ZERO;
  }

  private final ObjectMapper mapper;

  public JsonIndexFormatV03() {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  private @Nullable Instant instantFromInteger(@Nullable BigInteger millisecondsSinceEpoch) {
    return millisecondsSinceEpoch == null ? null : Instant.ofEpochMilli(millisecondsSinceEpoch.longValueExact());
  }

  @Override
  public BackupIndex load(InputStream data) throws IOException, MalformedDataException {
    final JsonIndex f;
    try {
      f = mapper.readValue(data, JsonIndex.class);
    } catch (JsonParseException e) {
      throw new MalformedDataException("Backup index is not legal JSON", e);
    } catch (JsonMappingException e) {
      throw new MalformedDataException("Backup index JSON is not well-formed", e);
    }
    BackupIndex index = new BackupIndex();
    for (JsonBlob blob : f.blobs) {
      index.addBackedUpBlob(
              new Sha256AndSize(Util.stringToSha256(blob.sha256), blob.size),
              new BackupReport(blob.backupId, blob.backupSize, blob.key));
    }
    for (var entry : f.history.entrySet()) {
      for (JsonBackupInfo b : entry.getValue()) {
        index.addBackupInfo(
                new SystemId(entry.getKey()),
                new BackupIndex.BackupMetadata(instantFromInteger(b.startTimeAsMillisecondsSinceEpoch), instantFromInteger(b.endTimeAsMillisecondsSinceEpoch), b.backupNumber));
      }
    }
    for (Map.Entry<String, Map<String, List<JsonRevision>>> entry : f.files.entrySet()) {
      SystemId system = new SystemId(entry.getKey());
      for (Map.Entry<String, List<JsonRevision>> entry2 : entry.getValue().entrySet()) {
        Path path = Paths.get(entry2.getKey());
        for (JsonRevision rev : entry2.getValue()) {
          final var backup = index.findBackup(system, rev.backupNumber);
          if (rev.sha256 != null) {
            Instant modTime = instantFromInteger(rev.modTimeAsMillisecondsSinceEpoch);
            index.appendRevision(system, backup, path, modTime, new Sha256AndSize(Util.stringToSha256(rev.sha256), rev.size));
          } else if (rev.symLinkDst != null) {
            Path p = Paths.get(rev.symLinkDst);
            index.appendRevision(system, backup, path, new SymLink(path, p));
          } else if (rev.hardLinkDst != null) {
            Path p = Paths.get(rev.hardLinkDst);
            index.appendRevision(system, backup, path, new HardLink(path, p));
          } else {
            index.appendTombstone(system, backup, path);
          }
        }
      }
    }
    index.setCleanupGeneration(f.cleanupGeneration);
    return index;
  }

  private JsonIndex convertToJSONSerializableObject(BackupIndex index) {
    JsonIndex result = new JsonIndex();

    index.knownSystems().forEach(system -> {
      for (var info : index.knownBackups(system)) {
        result.history.computeIfAbsent(system.toString(), s -> new ArrayList<>()).add(toJsonMeta(info));
      }
    });

    index.listBlobs().forEach(b -> {
      JsonBlob blob = new JsonBlob();
      BackupReport report = index.lookupBlob(b);
      assert report != null;
      blob.key = report.getKey();
      blob.sha256 = Util.sha256toString(b.getSha256());
      blob.size = b.getSize();
      blob.backupId = report.getIdAtTarget();
      blob.backupSize = report.getSizeAtTarget();
      result.blobs.add(blob);
    });

    index.knownSystems().forEach(system -> {
      Map<String, List<JsonRevision>> m = result.files.computeIfAbsent(system.toString(), s -> new HashMap<>());
      index.knownPaths(system).forEach(path -> {
        List<JsonRevision> l = m.computeIfAbsent(path.toString(), p -> new ArrayList<>());
        for (BackupIndex.Revision r : index.getInfo(system, path)) {
          l.add(toJsonRevision(r));
        }
      });
    });

    result.cleanupGeneration = index.getCleanupGeneration();

    return result;
  }

  private JsonBackupInfo toJsonMeta(BackupIndex.BackupMetadata info) {
    JsonBackupInfo res = new JsonBackupInfo();
    res.startTimeAsMillisecondsSinceEpoch = BigInteger.valueOf(info.getStartTime().toEpochMilli());
    res.endTimeAsMillisecondsSinceEpoch = info.getEndTime() == null ? null : BigInteger.valueOf(info.getEndTime().toEpochMilli());
    res.backupNumber = info.getBackupNumber();
    return res;
  }

  private static JsonRevision toJsonRevision(BackupIndex.Revision r) {
    JsonRevision res = new JsonRevision();
    res.backupNumber = r.backupNumber;
    switch (r.type) {
      case REGULAR_FILE:
        res.modTimeAsMillisecondsSinceEpoch = BigInteger.valueOf(r.modTime.toEpochMilli());
        res.sha256 = Util.sha256toString(r.summary.getSha256());
        res.size = r.summary.getSize();
        break;
      case SOFT_LINK:
        res.symLinkDst = r.linkTarget.toString();
        break;
      case HARD_LINK:
        res.hardLinkDst = r.linkTarget.toString();
        break;
      case TOMBSTONE:
        break;
      default:
        throw new UnsupportedOperationException(r.type.toString());
    }
    return res;
  }

  @Override
  public InputStream serialize(BackupIndex index) {
    return Util.createInputStream(out -> mapper.writeValue(out, convertToJSONSerializableObject(index)));
  }

}
