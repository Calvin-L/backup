package cal.bkup.types;

import java.math.BigDecimal;

public interface Price {

  Price ZERO = () -> BigDecimal.ZERO;

  BigDecimal valueInCents();

  default Price plus(Price p2) {
    final BigDecimal val = valueInCents().add(p2.valueInCents());
    return () -> val;
  }

}
