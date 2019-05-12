package cal.bkup.types;

import lombok.Value;

import java.util.List;

@Value
public class Config {
  SystemId systemName;
  List<Rule> backupRules;
}
