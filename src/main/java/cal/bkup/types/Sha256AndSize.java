package cal.bkup.types;

import cal.bkup.Util;

import java.util.Arrays;
import java.util.Objects;

public class Sha256AndSize {

  private final byte[] sha256;
  private final long size;

  public Sha256AndSize(byte[] sha256, long size) {
    Objects.requireNonNull(sha256);
    if (sha256.length != 32) {
      throw new IllegalArgumentException("array is the wrong length to be a sha256 checksum");
    }
    this.sha256 = sha256;
    this.size = size;
  }

  public byte[] sha256() { return sha256; }
  public long size() { return size; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Sha256AndSize that = (Sha256AndSize) o;
    return size == that.size &&
            Arrays.equals(sha256, that.sha256);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(size);
    result = 31 * result + Arrays.hashCode(sha256);
    return result;
  }

  @Override
  public String toString() {
    return "Sha256AndSize{" + Util.sha256toString(sha256) +
            ", size=" + size + '}';
  }
}
