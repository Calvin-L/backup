package cal.bkup.types;

import cal.bkup.Util;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public final class ResourceBundle {

  private final Collection<Resource> resources;

  public ResourceBundle(Collection<Resource> resources) {
    this.resources = Collections.unmodifiableCollection(new ArrayList<>(resources));
  }

  public Collection<Resource> resources() {
    return resources;
  }

  public InputStream open() throws IOException {
    return Util.createInputStream(stream -> {
      try (TarArchiveOutputStream out = new TarArchiveOutputStream(new XZOutputStream(stream, new LZMA2Options()))) {
        for (Resource r : resources) {
          TarArchiveEntry entry = new TarArchiveEntry(r.path().toAbsolutePath().toString());
          entry.setSize(r.sizeEstimateInBytes());
          out.putArchiveEntry(entry);
          try (InputStream in = r.open()) {
            Util.copyStream(in, out);
          }
          out.closeArchiveEntry();
        }
      }
    });
  }

  public long sizeEstimateInBytes() throws IOException {
    long sz = 0;
    for (Resource r : resources) {
      sz += r.sizeEstimateInBytes();
    }
    return sz;
  }

}
