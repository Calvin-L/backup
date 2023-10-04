package cal.prim.fs;

import cal.prim.IOConsumer;

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

  @Override
  public InputStream openRegularFileForReading(Path path) throws IOException {
    try {
      return Files.newInputStream(path);
    } catch (FileNotFoundException e) {
      // The JavaDoc for Files.newInputStream() does not specify which of the standard
      // library's two "file is missing" exceptions get thrown.  Since it belongs to the
      // `java.nio` package I suspect it will always throw NoSuchFileException, but just
      // in case, this block catches the other one and re-throws it as the one specified
      // by the JavaDoc contract for this method.
      throw new NoSuchFileException(path.toString());
    }
  }

  private static abstract class Visitor implements FileVisitor<Path> {
    private final Set<PathMatcher> exclusionRules;

    Visitor(Set<PathMatcher> exclusionRules) {
      this.exclusionRules = exclusionRules;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dirPath, BasicFileAttributes attrs) {
      Path dirName = dirPath.getFileName();
      return exclusionRules.stream().anyMatch(r -> r.matches(dirPath) || (dirName != null && r.matches(dirName))) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path filePath, BasicFileAttributes attrs) throws IOException {
      Path fileName = filePath.getFileName();
      if (fileName == null) {
        throw new IllegalArgumentException("No filename for path " + filePath);
      }
      if (exclusionRules.stream().allMatch(r -> !r.matches(filePath) && !r.matches(fileName))) {
        if (attrs.isSymbolicLink()) {
          Path dst = Files.readSymbolicLink(filePath);
          onSymLink(filePath, dst);
        } else {
          onFile(filePath, attrs);
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
