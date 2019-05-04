package cal.bkup.types;

import lombok.NonNull;
import lombok.Value;

import java.util.Objects;

@Value
public class Sha256AndSize {

  @NonNull byte[] sha256;
  long size;

  public Sha256AndSize(byte[] sha256, long size) {
    Objects.requireNonNull(sha256);
    if (sha256.length != 32) {
      throw new IllegalArgumentException("array is the wrong length to be a sha256 checksum");
    }
    this.sha256 = sha256;
    this.size = size;
  }

}
