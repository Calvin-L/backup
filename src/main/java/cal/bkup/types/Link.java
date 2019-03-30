package cal.bkup.types;

import java.nio.file.Path;
import java.util.Objects;

public class Link {

  private final Path src, dst;

  public Link(Path src, Path dst) {
    this.src = src;
    this.dst = dst;
  }

  public Path src() {
    return src;
  }

  public Path dst() {
    return dst;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Link link = (Link) o;
    return src.equals(link.src) && dst.equals(link.dst);
  }

  @Override
  public int hashCode() {
    return Objects.hash(src, dst);
  }

}
