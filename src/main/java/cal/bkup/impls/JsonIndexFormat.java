package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.BackupReport;
import cal.bkup.types.HardLink;
import cal.bkup.types.Id;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SymLink;
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

public class JsonIndexFormat implements IndexFormat {

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

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public BackupIndex load(InputStream data) throws IOException {
    Format f = mapper.readValue(data, Format.class);
    BackupIndex index = new BackupIndex();
    for (JsonBlob blob : f.blobs) {
      index.addBackedUpBlob(new Sha256AndSize(Util.stringToSha256(blob.sha256), blob.size), blob.key, new BackupReport() {
        @Override
        public Id idAtTarget() {
          return new Id(blob.backupId);
        }

        @Override
        public long sizeAtTarget() {
          return blob.backupSize;
        }
      });
    }
    for (Map.Entry<String, Map<String, List<Revision>>> entry : f.files.entrySet()) {
      Id system = new Id(entry.getKey());
      for (Map.Entry<String, List<Revision>> entry2 : entry.getValue().entrySet()) {
        Path path = Paths.get(entry2.getKey());
        for (Revision rev : entry2.getValue()) {
          if (rev.sha256 != null) {
            Instant modTime = Instant.ofEpochMilli(rev.modTimeAsMillisecondsSinceEpoch.longValueExact());
            index.appendRevision(system, path, modTime, new Sha256AndSize(Util.stringToSha256(rev.sha256), rev.size));
          } else if (rev.symLinkDst != null) {
            Path p = Paths.get(rev.symLinkDst);
            index.appendRevision(system, path, new SymLink(path, p));
          } else if (rev.hardLinkDst != null) {
            Path p = Paths.get(rev.hardLinkDst);
            index.appendRevision(system, path, new HardLink(path, p));
          } else {
            index.appendTombstone(system, path);
          }
        }
      }
    }
    return index;
  }

  private Format simplify(BackupIndex index) {
    Format result = new Format();

    index.listBlobs().forEach(b -> {
      JsonBlob blob = new JsonBlob();
      BackupIndex.BackupReportAndKey report = index.lookupBlob(b);
      assert report != null;
      blob.key = report.key;
      blob.sha256 = Util.sha256toString(b.sha256());
      blob.size = b.size();
      blob.backupId = report.idAtTarget().toString();
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
    switch (r.type) {
      case REGULAR_FILE:
        res.modTimeAsMillisecondsSinceEpoch = BigInteger.valueOf(r.modTime.toEpochMilli());
        res.sha256 = Util.sha256toString(r.summary.sha256());
        res.size = r.summary.size();
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
    return Util.createInputStream(out -> mapper.writeValue(out, simplify(index)));
  }

}
