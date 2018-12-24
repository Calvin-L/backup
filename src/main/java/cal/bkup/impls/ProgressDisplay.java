package cal.bkup.impls;

import cal.bkup.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProgressDisplay implements AutoCloseable {

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

  public interface ProgressCallback {
    void reportProgress(long numerator, long denominator);
  }

  private static void moveCursorUpOneLine() throws IOException {
    Util.run("tput", "cuu1");
//    System.out.println("^1");
  }

  private static void clearCurrentLine() throws IOException {
    Util.run("tput", "el");
//    System.out.println("clr");
  }

  private static void clearLines(int n) throws IOException {
    for (int i = 0; i < n; ++i) {
      moveCursorUpOneLine();
      clearCurrentLine();
    }
  }

  private final List<Task> tasks;

  public ProgressDisplay() throws IOException {
    Util.run("tput", "rmam");
    tasks = new ArrayList<>();
  }

  private void printTask(Task task) {
    System.out.println("[" + String.format("%2d", task.progress * 100 / task.denominator) + "%] " + task.description);
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

  private void printTasksFromIndex(int index) throws IOException {
    for (int i = index; i < tasks.size(); ++i) {
      printTask(tasks.get(i));
    }
  }

  public synchronized void reportProgress(Task task, long progress, long denominator) throws IOException {
    int index = findTask(task);
    task.progress = progress;
    task.denominator = denominator;
    clearLines(tasks.size() - index);
    printTasksFromIndex(index);
  }

  public synchronized void finishTask(Task task) throws IOException {
    int index = findTask(task);
    clearLines(tasks.size() - index);
    tasks.remove(index);
    printTasksFromIndex(index);
  }

  @Override
  public synchronized void close() throws IOException {
    clearLines(tasks.size());
    Util.run("tput", "smam");
    tasks.clear();
  }

}
