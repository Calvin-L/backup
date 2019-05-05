package cal.bkup;

import cal.bkup.types.Config;
import cal.prim.fs.HardLink;
import cal.prim.IOConsumer;
import cal.prim.fs.RegularFile;
import cal.bkup.types.Rule;
import cal.prim.fs.SymLink;
import cal.prim.fs.Filesystem;
import cal.prim.fs.PhysicalFilesystem;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class FileTools {

  public static void forEachFile(Config config, IOConsumer<SymLink> symlinkConsumer, IOConsumer<HardLink> hardLinkConsumer, IOConsumer<RegularFile> consumer) throws IOException {
    Filesystem fs = new PhysicalFilesystem();

    List<Rule> rules = config.getBackupRules();
    Set<PathMatcher> exclusions = new LinkedHashSet<>();

    // It is important that this is a sorted set.  This
    // gives us deterministic hard link detection.
    SortedSet<RegularFile> regularFiles = new TreeSet<>(Comparator.comparing(RegularFile::getPath));

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
      Path canonical = canonicalPathForEachInode.putIfAbsent(f.getINode(), f.getPath());
      if (canonical == null) {
        consumer.accept(f);
      } else {
        hardLinkConsumer.accept(new HardLink(f.getPath(), canonical));
      }
    }

  }

}
