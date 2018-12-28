package cal.bkup;

import cal.bkup.types.Config;
import cal.bkup.types.HardLink;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
import cal.bkup.types.Rule;
import cal.bkup.types.SymLink;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class FileTools {
  public static void forEachFile(Config config, IOConsumer<SymLink> symlinkConsumer, IOConsumer<HardLink> hardLinkConsumer, IOConsumer<Resource> consumer) throws IOException {
    Collection<Path> paths = new HashSet<>();

    // TODO: this will work best if we have a deterministic exploration order :/
    Map<Long, Path> inodes = new HashMap<>();
    List<Rule> rules = config.backupRules();

    POSIX posix = POSIXFactory.getPOSIX();
    assert posix.isNative();

    for (int i = 0; i < rules.size(); ++i) {
      Rule r = rules.get(i);

      int index = i;
      r.destruct(
          include -> {
            List<PathMatcher> exclusions = new ArrayList<>();
            rules.subList(index + 1, rules.size())
                .forEach(rr -> {
                  try {
                    rr.destruct(inc -> {}, exclusions::add);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
            Files.walkFileTree(include, new Visitor(exclusions) {
              @Override
              protected void onFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (paths.add(path)) {
                  long inode = posix.stat(path.toString()).ino();
                  Path preexisting = inodes.get(inode);

                  if (preexisting != null) {
                    hardLinkConsumer.accept(new HardLink() {
                      @Override
                      public Path src() {
                        return path;
                      }

                      @Override
                      public Path dst() {
                        return preexisting;
                      }
                    });
                  } else {
                    inodes.put(inode, path);
                    long size = attrs.size();
                    consumer.accept(new Resource() {
                      @Override
                      public Id system() {
                        return config.systemName();
                      }

                      @Override
                      public Path path() {
                        return path;
                      }

                      @Override
                      public Instant modTime() throws IOException {
                        return Files.getLastModifiedTime(path).toInstant();
                      }

                      @Override
                      public InputStream open() throws IOException {
                        return Files.newInputStream(path);
                      }

                      @Override
                      public long sizeEstimateInBytes() {
                        return size;
                      }
                    });
                  }
                }
              }

              @Override
              protected void onSymLink(Path src, Path dst) throws IOException {
                if (paths.add(src)) {
                  symlinkConsumer.accept(new SymLink() {
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
            });
          },
          exclude -> { });
    }
  }

  public static abstract class Visitor implements FileVisitor<Path> {
    private List<PathMatcher> exclusionRules;

    public Visitor(List<PathMatcher> exclusionRules) {
      this.exclusionRules = exclusionRules;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      return exclusionRules.stream().anyMatch(r -> r.matches(dir) || r.matches(dir.getFileName())) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if (exclusionRules.stream().allMatch(r -> !r.matches(file) && !r.matches(file.getFileName()))) {
        if (attrs.isSymbolicLink()) {
          Path dst = Files.readSymbolicLink(file);
          onSymLink(file, dst);
        } else {
          onFile(file, attrs);
        }
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      System.err.println("failed to visit: " + file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      return FileVisitResult.CONTINUE;
    }

    protected abstract void onFile(Path file, BasicFileAttributes attrs) throws IOException;
    protected abstract void onSymLink(Path src, Path dst) throws IOException;
  }
}
