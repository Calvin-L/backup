package cal.prim;

import java.util.Objects;

public class Pair<A, B> {
  public final A fst;
  public final B snd;

  public Pair(A fst, B snd) {
    this.fst = fst;
    this.snd = snd;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Pair<?, ?> pair = (Pair<?, ?>) o;
    return Objects.equals(fst, pair.fst) && Objects.equals(snd, pair.snd);
  }

  @Override
  public int hashCode() {
    int result = fst != null ? fst.hashCode() : 0;
    result = 31 * result + (snd != null ? snd.hashCode() : 0);
    return result;
  }
}
