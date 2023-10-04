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

@SuppressWarnings({"initialization.field.uninitialized", "dereference.of.nullable"}) // TODO
public class JsonIndexFormatV01 implements IndexFormat {

  private final Instant UNKNOWN_TIME = Instant.EPOCH;

  private static class Revision {
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

  private static class Format {
    public Map<String, Map<String, List<Revision>>> files = new HashMap<>();
    public Set<JsonBlob> blobs = new HashSet<>();
  }

  private final ObjectMapper mapper;

  public JsonIndexFormatV01() {
    mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  @Override
  public BackupIndex load(InputStream data) throws IOException, MalformedDataException {
    final Format f;
    try {
      f = mapper.readValue(data, Format.class);
    } catch (JsonParseException e) {
      throw new MalformedDataException("Backup index is not legal JSON", e);
    } catch (JsonMappingException e) {
      throw new MalformedDataException("Backup index JSON is not well-formed", e);
    }
    BackupIndex index = new BackupIndex();
    for (JsonBlob blob : f.blobs) {
      index.addBackedUpBlob(
              new Sha256AndSize(Util.stringToSha256(blob.sha256), blob.size),
              new BackupReport(blob.backupId, 0, blob.backupSize, blob.key));
    }
    Map<SystemId, BackupIndex.BackupMetadata> meta = new HashMap<>();
    for (Map.Entry<String, Map<String, List<Revision>>> entry : f.files.entrySet()) {
      SystemId system = new SystemId(entry.getKey());
      final var backup = meta.computeIfAbsent(system, s -> index.startBackup(system, UNKNOWN_TIME));
      for (Map.Entry<String, List<Revision>> entry2 : entry.getValue().entrySet()) {
        Path path = Paths.get(entry2.getKey());
        for (Revision rev : entry2.getValue()) {
          if (rev.sha256 != null) {
            Instant modTime = Instant.ofEpochMilli(rev.modTimeAsMillisecondsSinceEpoch.longValueExact());
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
    for (var entry : meta.entrySet()) {
      index.finishBackup(entry.getKey(), entry.getValue(), UNKNOWN_TIME);
    }
    return index;
  }

  private Format simplify(BackupIndex index) {
    Format result = new Format();

    index.listBlobs().forEach(b -> {
      JsonBlob blob = new JsonBlob();
      BackupReport report = index.lookupBlob(b);
      assert report != null;
      blob.key = report.key();
      blob.sha256 = Util.sha256toString(b.sha256());
      blob.size = b.size();
      blob.backupId = report.idAtTarget();
      blob.backupSize = report.sizeAtTarget();
      result.blobs.add(blob);
    });

    index.knownSystems().forEach(system -> {
      Map<String, List<Revision>> m = result.files.computeIfAbsent(system.toString(), s -> new HashMap<>());
      index.knownPaths(system).forEach(path -> {
        List<Revision> l = m.computeIfAbsent(path.toString(), p -> new ArrayList<>());
        for (BackupIndex.Revision r : index.getInfo(system, path)) {
          l.add(toJsonRevision(r));
        }
      });
    });

    return result;
  }

  private static Revision toJsonRevision(BackupIndex.Revision r) {
    Revision res = new Revision();
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
    return Util.createInputStream(out -> mapper.writeValue(out, simplify(index)));
  }

}
