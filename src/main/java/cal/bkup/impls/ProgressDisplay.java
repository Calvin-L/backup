package cal.bkup.impls;

import cal.prim.QuietAutoCloseable;
import cal.prim.RateLimitedRunnable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class ProgressDisplay implements QuietAutoCloseable {

  public static class Task {
    private String description;
    private long progress;
    private long denominator;

    private Task(String description) {
      this.description = description;
      this.progress = 0L;
      this.denominator = 1L;
    }
  }

  private long totalComplete;
  private final long totalTasks;
  private final List<Task> tasks;
  private final Runnable refreshDisplay;

  public ProgressDisplay(long totalTasks) {
    tasks = new ArrayList<>();
    this.totalTasks = totalTasks;
    totalComplete = 0L;
    refreshDisplay = new RateLimitedRunnable(Duration.ofMinutes(1), RateLimitedRunnable.Mode.RUN_ON_FIRST_CALL, () -> {
      for (Task t : tasks) {
        printTask(t);
      }
    });
  }

  private static String formatPercent(long numerator, long denominator) {
    if (numerator < 0) {
      throw new IllegalArgumentException("negative numerator: " + numerator);
    }
    if (denominator < 0) {
      throw new IllegalArgumentException("negative denominator: " + denominator);
    }
    if (numerator > denominator) {
      return "100%";
    }
    return String.format("%3d", numerator * 100 / denominator) + '%';
  }

  private void printTask(Task task) {
    System.out.println('[' + formatPercent(totalComplete, totalTasks) + '/' + formatPercent(task.progress, task.denominator) + "] " + task.description);
  }

  private void printSkippedTask(String taskDescription, String why) {
    System.out.println('[' + formatPercent(totalComplete, totalTasks) + '/' + formatPercent(1L, 1L) + "] skipped: " + taskDescription + " (" + why + ')');
  }

  public synchronized Task startTask(String description) {
    Task t = new Task(description);
    tasks.add(t);
    printTask(t);
    return t;
  }

  private int findTask(Task task) {
    int index = tasks.indexOf(task);
    if (index < 0) {
      throw new IllegalArgumentException("unknown task " + task);
    }
    return index;
  }

  public synchronized void reportProgress(Task task, long progress, long denominator) {
    task.progress = progress;
    task.denominator = denominator;
    refreshDisplay.run();
  }

  public synchronized void finishTask(Task task) {
    ++totalComplete;
    int index = findTask(task);
    tasks.remove(index);
    task.progress = task.denominator = 1L;
    printTask(task);
  }

  public synchronized void skipTask(String taskDescription, String why) {
    ++totalComplete;
    printSkippedTask(taskDescription, why);
    refreshDisplay.run();
  }

  @Override
  public synchronized void close() {
    tasks.clear();
  }

}
