package cal.prim;

import org.apache.commons.math3.fraction.BigFraction;

public interface Price {

  Price ZERO = () -> BigFraction.ZERO;

  BigFraction valueInCents();

  default Price plus(Price p2) {
    final BigFraction val = valueInCents().add(p2.valueInCents());
    return () -> val;
  }

}
