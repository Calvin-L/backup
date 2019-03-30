package cal.prim.fs;

import cal.prim.Pair;

import java.nio.file.Path;

public class Link extends Pair<Path, Path> {
  public Link(Path fst, Path snd) {
    super(fst, snd);
  }

  public Path src() {
    return fst;
  }

  public Path dst() {
    return snd;
  }
}
