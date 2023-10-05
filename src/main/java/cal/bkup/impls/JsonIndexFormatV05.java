package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SystemId;
import cal.prim.MalformedDataException;
import cal.prim.fs.HardLink;
import cal.prim.fs.SymLink;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

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

public class JsonIndexFormatV05 implements IndexFormat {

  private static class JsonBackupInfo {
    public @Nullable BigInteger startTimeAsMillisecondsSinceEpoch;
    public @Nullable BigInteger endTimeAsMillisecondsSinceEpoch;
    public long backupNumber;
  }

  private static class JsonRevision {
    public long backupNumber;
    public @Nullable BigInteger modTimeAsMillisecondsSinceEpoch;
    public @Nullable String sha256;
    public long size;
    public @Nullable String symLinkDst;
    public @Nullable String hardLinkDst;
  }

  private static class JsonBlob {
    public @Nullable String key;
    public @Nullable String sha256;
    public long size;
    public @Nullable String backupId;
    public long backupOffset;
    public long backupSize;

    public String getKey() throws MalformedDataException {
      String key = this.key;
      if (key == null) {
        throw new MalformedDataException("Unidentified blob has no key");
      }
      return key;
    }

    public String getSha256() throws MalformedDataException {
      String sha256 = this.sha256;
      if (sha256 == null) {
        throw new MalformedDataException("Blob " + key + " has no sha256");
      }
      return sha256;
    }

    public String getBackupId() throws MalformedDataException {
      String backupId = this.backupId;
      if (backupId == null) {
        throw new MalformedDataException("Blob " + key + " has no backupId");
      }
      return backupId;
    }
  }

  private static class JsonIndex {
    public Map<String, List<JsonBackupInfo>> history = new HashMap<>();
    public Map<String, Map<String, List<JsonRevision>>> files = new HashMap<>();
    public Set<JsonBlob> blobs = new HashSet<>();
    public BigInteger cleanupGeneration = BigInteger.ZERO;
  }

  private final ObjectMapper mapper;

  public JsonIndexFormatV05() {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  private @PolyNull Instant instantFromInteger(@PolyNull BigInteger millisecondsSinceEpoch) {
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
          new Sha256AndSize(Util.stringToSha256(blob.getSha256()), blob.size),
          new BackupReport(blob.getBackupId(), blob.backupOffset, blob.backupSize, blob.getKey()));
    }
    for (var entry : f.history.entrySet()) {
      for (JsonBackupInfo b : entry.getValue()) {
        var startTime = b.startTimeAsMillisecondsSinceEpoch;
        if (startTime == null) {
          throw new MalformedDataException("Backup #" + b.backupNumber + " has no start time");
        }
        index.addBackupInfo(
            new SystemId(entry.getKey()),
            new BackupIndex.BackupMetadata(instantFromInteger(startTime), instantFromInteger(b.endTimeAsMillisecondsSinceEpoch), b.backupNumber));
      }
    }
    for (Map.Entry<String, Map<String, List<JsonRevision>>> entry : f.files.entrySet()) {
      SystemId system = new SystemId(entry.getKey());
      for (Map.Entry<String, List<JsonRevision>> entry2 : entry.getValue().entrySet()) {
        Path path = Paths.get(entry2.getKey());
        for (JsonRevision rev : entry2.getValue()) {
          final var backup = index.findBackup(system, rev.backupNumber);
          final var revSha256 = rev.sha256;
          if (revSha256 != null) {
            Instant modTime = instantFromInteger(rev.modTimeAsMillisecondsSinceEpoch);
            if (modTime == null) {
              throw new MalformedDataException("Revision " + revSha256 + " for " + path + " has no modification time");
            }
            try {
              index.appendRevision(system, backup, path, modTime, new Sha256AndSize(Util.stringToSha256(revSha256), rev.size));
            } catch (IllegalArgumentException exn) {
              System.err.println("WARNING: missing content for file " + system + ":" + path + "; this may indicate a corrupted index!");
            }
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
      if (report == null) {
        throw new IllegalStateException("Index lists blob " + b + " but has no report for it");
      }
      blob.key = report.key();
      blob.sha256 = Util.sha256toString(b.sha256());
      blob.size = b.size();
      blob.backupId = report.idAtTarget();
      blob.backupOffset = report.offsetAtTarget();
      blob.backupSize = report.sizeAtTarget();
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
    res.startTimeAsMillisecondsSinceEpoch = BigInteger.valueOf(info.startTime().toEpochMilli());
    var endTime = info.endTime();
    res.endTimeAsMillisecondsSinceEpoch = endTime == null ? null : BigInteger.valueOf(endTime.toEpochMilli());
    res.backupNumber = info.backupNumber();
    return res;
  }

  private static JsonRevision toJsonRevision(BackupIndex.Revision r) {
    JsonRevision res = new JsonRevision();
    res.backupNumber = r.backupNumber();
    // NOTE: could use pattern-matching switch in future JDKs
    if (r instanceof BackupIndex.RegularFileRev f) {
      res.modTimeAsMillisecondsSinceEpoch = BigInteger.valueOf(f.modTime().toEpochMilli());
      res.sha256 = Util.sha256toString(f.summary().sha256());
      res.size = f.summary().size();
    } else if (r instanceof BackupIndex.SoftLinkRev l) {
      res.symLinkDst = l.target().toString();
    } else if (r instanceof BackupIndex.HardLinkRev l) {
      res.hardLinkDst = l.target().toString();
    } else if (!(r instanceof BackupIndex.TombstoneRev)) {
      throw new UnsupportedOperationException(r.toString());
    }
    return res;
  }

  @Override
  public InputStream serialize(BackupIndex index) {
    return Util.createInputStream(out -> mapper.writeValue(out, convertToJSONSerializableObject(index)));
  }

}
