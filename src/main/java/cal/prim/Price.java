package cal.prim;

import lombok.Value;
import org.apache.commons.math3.fraction.BigFraction;

@Value
public class Price {
  public static Price ZERO = new Price(BigFraction.ZERO);

  BigFraction valueInCents;

  public Price plus(Price p2) {
    return new Price(getValueInCents().add(p2.getValueInCents()));
  }

  @Override
  public String toString() {
    long pennies = valueInCents.longValue();
    boolean pos = true;
    if (pennies < 0) {
      pos = false;
      pennies = -pennies;
    }
    return (pos ? "" : "-") + '$' + (pennies / 100) + '.' + (pennies % 100 / 10) + (pennies % 100 % 10);
  }

}
