package cal.prim;

import org.apache.commons.math3.fraction.BigFraction;

public record Price(BigFraction valueInCents) {
  public static Price ZERO = new Price(BigFraction.ZERO);

  public Price plus(Price p2) {
    return new Price(valueInCents.add(p2.valueInCents));
  }

  public Price negate() {
    return new Price(valueInCents.negate());
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
