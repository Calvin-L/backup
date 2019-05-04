package cal.prim;

import lombok.Value;

@Value
public class Pair<A, B> {
  A fst;
  B snd;
}
