package cal.prim;

import cal.bkup.types.IOConsumer;
import cal.bkup.types.RegularFile;
import cal.bkup.types.SymLink;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public class PhysicalFilesystem implements Filesystem {

  @Override
  public void scan(Path path, Set<PathMatcher> exclusions, IOConsumer<SymLink> onSymlink, IOConsumer<RegularFile> onFile) throws IOException {
    POSIX posix = POSIXFactory.getPOSIX();
    assert posix.isNative();

    Files.walkFileTree(path, new Visitor(exclusions) {
      @Override
      protected void onFile(Path path, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        Instant modTime = attrs.lastModifiedTime().toInstant();
        Object inode = Objects.requireNonNull(attrs.fileKey());

        onFile.accept(new RegularFile() {
          @Override
          public Path path() {
            return path;
          }

          @Override
          public Instant modTime() {
            return modTime;
          }

          @Override
          public InputStream open() throws IOException {
            try {
              return Files.newInputStream(path);
            } catch (FileNotFoundException e) {
              // The JavaDoc for Files.newInputStream() does not specify which of the standard
              // library's two "file is missing" exceptions get thrown.  Since it belongs to the
              // `java.nio` package I suspect it will always throw NoSuchFileException, but just
              // in case, this block catches the other one and re-throws it as the one specified
              // by the JavaDoc contract for RegularFile.open().
              throw new NoSuchFileException(path.toString());
            }
          }

          @Override
          public long sizeEstimateInBytes() {
            return size;
          }

          @Override
          public Object inode() {
            return inode;
          }
        });
      }

      @Override
      protected void onSymLink(Path src, Path dst) throws IOException {
        onSymlink.accept(new SymLink() {
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
    });
  }

  private static abstract class Visitor implements FileVisitor<Path> {
    private Set<PathMatcher> exclusionRules;

    Visitor(Set<PathMatcher> exclusionRules) {
      this.exclusionRules = exclusionRules;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
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
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      System.err.println("failed to visit: " + file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      return FileVisitResult.CONTINUE;
    }

    protected abstract void onFile(Path file, BasicFileAttributes attrs) throws IOException;
    protected abstract void onSymLink(Path src, Path dst) throws IOException;
  }

}
