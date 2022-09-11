package cal.bkup.types;

import java.util.Arrays;
import java.util.Objects;

public record Sha256AndSize(byte[] sha256, long size) {

  public Sha256AndSize {
    Objects.requireNonNull(sha256);
    if (sha256.length != 32) {
      throw new IllegalArgumentException("array is the wrong length to be a sha256 checksum");
    }
  }

  // NOTE: Arrays use reference equality, so we need our own equals() and hashCode()

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Sha256AndSize other &&
            size == other.size &&
            Arrays.equals(sha256, other.sha256);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(sha256) * 31 + Long.hashCode(size);
  }

}
