package cal.bkup.types;

import java.util.List;

public interface Config {
  Id systemName();
  List<Rule> backupRules();
}
