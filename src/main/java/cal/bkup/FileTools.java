package cal.bkup;

import cal.bkup.types.Config;
import cal.bkup.types.HardLink;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.RegularFile;
import cal.bkup.types.Resource;
import cal.bkup.types.Rule;
import cal.bkup.types.SymLink;
import cal.prim.Filesystem;
import cal.prim.PhysicalFilesystem;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class FileTools {

  public static void forEachFile(Config config, IOConsumer<SymLink> symlinkConsumer, IOConsumer<HardLink> hardLinkConsumer, IOConsumer<Resource> consumer) throws IOException {
    Filesystem fs = new PhysicalFilesystem();

    List<Rule> rules = config.backupRules();
    Set<PathMatcher> exclusions = new LinkedHashSet<>();

    // It is important that this is a sorted set.  This
    // gives us deterministic hard link detection.
    SortedSet<RegularFile> regularFiles = new TreeSet<>(Comparator.comparing(RegularFile::path));

    // (1) scan the filesystem
    for (int i = rules.size() - 1; i >= 0; --i) {
      rules.get(i).destruct(
              (pathToInclude) -> {
                if (!pathToInclude.isAbsolute()) {
                  throw new IllegalArgumentException("not an absolute path: " + pathToInclude);
                }
                fs.scan(pathToInclude, exclusions, symlinkConsumer, regularFiles::add);
                exclusions.add(path -> path.equals(pathToInclude));
              },
              exclusions::add
      );
    }

    // (2) hardlink detection
    Map<Object, Path> canonicalPathForEachInode = new HashMap<>();
    for (RegularFile f : regularFiles) {
      Path canonical = canonicalPathForEachInode.putIfAbsent(f.inode(), f.path());
      if (canonical == null) {
        consumer.accept(new Resource() {
          @Override
          public Id system() {
            return config.systemName();
          }

          @Override
          public Path path() {
            return f.path();
          }

          @Override
          public Instant modTime() {
            return f.modTime();
          }

          @Override
          public InputStream open() throws IOException {
            return f.open();
          }

          @Override
          public long sizeEstimateInBytes() {
            return f.sizeEstimateInBytes();
          }

          @Override
          public Object inode() {
            return f.inode();
          }
        });
      } else {
        hardLinkConsumer.accept(new HardLink() {
          @Override
          public Path src() {
            return f.path();
          }

          @Override
          public Path dst() {
            return canonical;
          }
        });
      }
    }

  }

}
