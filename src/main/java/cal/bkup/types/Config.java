package cal.bkup.types;

import java.util.List;

public record Config(SystemId systemName, List<Rule> backupRules) {
}
