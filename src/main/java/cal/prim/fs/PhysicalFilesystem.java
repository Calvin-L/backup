package cal.prim.fs;

import cal.prim.IOConsumer;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;

public class PhysicalFilesystem implements Filesystem {

  @Override
  public void scan(Path path, Set<PathMatcher> exclusions, IOConsumer<SymLink> onSymlink, IOConsumer<RegularFile> onFile) throws IOException {
    Files.walkFileTree(path, new Visitor(exclusions) {
      @Override
      protected void onFile(Path path, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        Instant modTime = attrs.lastModifiedTime().toInstant();
        Object inode = Objects.requireNonNull(attrs.fileKey());
        onFile.accept(new RegularFile(path, modTime, size, inode));
      }

      @Override
      protected void onSymLink(Path src, Path dst) throws IOException {
        onSymlink.accept(new SymLink(src, dst));
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
