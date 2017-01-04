package cal.bkup;

import cal.bkup.impls.DummyCheckpoint;
import cal.bkup.impls.DummyTarget;
import cal.bkup.impls.EncryptedDirectory;
import cal.bkup.impls.EncryptedInputStream;
import cal.bkup.impls.GlacierBackupTarget;
import cal.bkup.impls.S3Directory;
import cal.bkup.impls.SqliteCheckpoint;
import cal.bkup.impls.XZCompressedDirectory;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
import cal.bkup.types.SimpleDirectory;
import cal.bkup.types.SymLink;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.Console;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

  private static final String AWS_REGION = "us-east-2";
  private static final String GLACIER_VAULT_NAME = "mybackups";
  private static final String GLACIER_ENDPOINT = "glacier." + AWS_REGION + ".amazonaws.com";
  private static final String S3_BUCKET = "backupindex";
  private static final String S3_ENDPOINT = "s3." + AWS_REGION + ".amazonaws.com";
  private static final int BACKLOG_CAPACITY = 8;
  private static final int NTHREADS = Runtime.getRuntime().availableProcessors();
  private static final Id SYSTEM_ID = new Id("UWLaptop");
  private static final List<String> RULES = Arrays.asList(
//      "+ ~/Documents",
//      "+ ~/sources",
      "- ~/sources/opensource",
      "- ~/sources/**/build",
      "- *.o",
      "- *.class",
      "- *.jar",
      "- *.hi",
      "- *.vo",
      "- *.glob",
      "- *.pyc",
      "- *.egg",
      "+ ~/xfer",
      "- ~/xfer/penlinux.img",
      "+ ~/.bash_profile",
      "- .stack-work",
      "- *.log",
      "+ ~/Pictures",
      "- .DS_Store",
      "- .fseventsd",
      "- .Spotlight-V100",
      "- .Trashes",
      "- Thumbs.db");

  private enum UseDummy { USE_DUMMY, USE_REAL };

  private static final AtomicLong numBackedUp = new AtomicLong(0);
  private static final AtomicLong numSkipped = new AtomicLong(0);
  private static final AtomicLong numErrs = new AtomicLong(0);

  private static void showHelp(Options options) {
    new HelpFormatter().printHelp("backup [options]", options);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Show help and quit");

    OptionGroup action = new OptionGroup();
    action.addOption(new Option("d", "dry-run", false, "Show what would be done"));
    action.addOption(new Option("l", "list", false, "Show inventory of current backup"));
    options.addOptionGroup(action);

    CommandLine cli;
    try {
      cli = new DefaultParser().parse(options, args);
    } catch (ParseException e) {
      System.err.println("Failed to parse options: " + e);
      showHelp(options);
      System.exit(1);
      return;
    }

    if (cli.hasOption('h')) {
      showHelp(options);
      return;
    }

    boolean dryRun = cli.hasOption('d');
    boolean list = cli.hasOption('l');

    Console cons;
    String password;
    if ((cons = System.console()) != null &&
        (password = readPassword(cons)) != null) {
      if (list) {
        try (Checkpoint checkpoint = findMostRecentCheckpoint(password, UseDummy.USE_DUMMY)) {
          checkpoint.list().forEach(info -> {
            System.out.println("/" + info.system() + info.path() + " [" + info.target() + '/' + info.idAtTarget() + '/' + info.modTime() + ']');
          });
          checkpoint.symlinks().forEach(link -> {
            System.out.println("/" + link.src() + " ----> " + link.dst());
          });
        }
      } else {
        try (Checkpoint checkpoint = findMostRecentCheckpoint(password, dryRun ? UseDummy.USE_DUMMY : UseDummy.USE_REAL);
             BackupTarget target = getBackupTarget(password, checkpoint, dryRun ? UseDummy.USE_DUMMY : UseDummy.USE_REAL)) {
          System.out.println("Backup started");
          try {
            forEachFile(
                symlink -> {
                  System.out.println("Symlink: " + symlink.src() + " --> " + symlink.dst());
                  checkpoint.noteSymLink(SYSTEM_ID, symlink);
                },
                resource -> {
                  System.out.println("Backing up " + resource.path().getFileName());
                  target.backup(resource,
                      id -> {
                        numBackedUp.incrementAndGet();
                        System.out.println("Finished " + resource.path());
                        checkpoint.noteSuccessfulBackup(resource, target, id);
                        synchronized (checkpoint) {
                          Instant lastSave = checkpoint.lastSave();
                          if (lastSave == null || lastSave.compareTo(Instant.now().minus(5, ChronoUnit.MINUTES)) < 0) {
                            checkpoint.save();
                          }
                        }
                      });
                });
          } finally {
            long nbkup = numBackedUp.get();
            System.out.println(nbkup + " backed up, " + numSkipped + " skipped, " + numErrs + " errors");
            if (nbkup > 0L) {
              checkpoint.save();
            }
          }
        }
      }
    } else {
      throw new Exception("failed to get password");
    }
  }

  private static String readPassword(Console cons) {
    char[] c1 = cons.readPassword("[%s]", "Password:");
    if (c1 == null) return null;
    char[] c2 = cons.readPassword("[%s]", "Confirm:");
    if (c2 == null) return null;
    if (!Arrays.equals(c1, c2)) {
      System.err.println("passwords do not match");
      return null;
    }
    return new String(c1);
  }

  private static Checkpoint findMostRecentCheckpoint(String password, UseDummy dummy) throws IOException {
    try {
//      SimpleDirectory dir = new EncryptedDirectory(new XZCompressedDirectory(LocalDirectory.TMP), password);
//      SimpleDirectory dir = LocalDirectory.TMP;
      SimpleDirectory dir = new EncryptedDirectory(new XZCompressedDirectory(new S3Directory(S3_BUCKET, S3_ENDPOINT)), password);
      Checkpoint res = new SqliteCheckpoint(dir);
      if (dummy == UseDummy.USE_DUMMY) res = new DummyCheckpoint(res);
      return res;
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private static BackupTarget getBackupTarget(String password, Checkpoint checkpoint, UseDummy useDummy) throws GeneralSecurityException, UnsupportedEncodingException {
//    BackupTarget baseTarget = new FilesystemBackupTarget(Paths.get("/tmp/bkup"));
    BackupTarget baseTarget = new GlacierBackupTarget(GLACIER_ENDPOINT, GLACIER_VAULT_NAME);
    if (useDummy == UseDummy.USE_DUMMY) baseTarget = new DummyTarget(baseTarget);
    return checkModTime(bufferTarget(encryptTarget(baseTarget, password)), checkpoint);
  }

  private static BackupTarget checkModTime(BackupTarget backupTarget, Checkpoint checkpoint) {
    return new BackupTarget() {
      @Override
      public Id name() {
        return backupTarget.name();
      }

      @Override
      public void backup(Resource r, IOConsumer<Id> k) throws IOException {
        Instant checkpointModTime = checkpoint.modTime(r, backupTarget);
        if (checkpointModTime == null || r.modTime().compareTo(checkpointModTime) > 0) {
          backupTarget.backup(r, k);
        } else {
          System.out.println("Skipping " + r.path());
          numSkipped.incrementAndGet();
        }
      }

      @Override
      public void close() throws Exception {
        backupTarget.close();
      }
    };
  }

  private static BackupTarget encryptTarget(BackupTarget backupTarget, String password) throws GeneralSecurityException, UnsupportedEncodingException {
    return new BackupTarget() {
      @Override
      public Id name() {
        return backupTarget.name();
      }

      @Override
      public void backup(Resource r, IOConsumer<Id> k) throws IOException {
        backupTarget.backup(new Resource() {
          @Override
          public Id system() {
            return r.system();
          }

          @Override
          public Path path() {
            return r.path();
          }

          @Override
          public Instant modTime() throws IOException {
            return r.modTime();
          }

          @Override
          public InputStream open() throws IOException {
            try {
              return new EncryptedInputStream(r.open(), password);
            } catch (GeneralSecurityException e) {
              throw new IOException(e);
            }
          }
        }, k);
      }

      @Override
      public void close() throws Exception {
        backupTarget.close();
      }
    };
  }

  private static BackupTarget bufferTarget(BackupTarget backupTarget) {
    BlockingQueue<IOConsumer<Void>> jobs = new ArrayBlockingQueue<>(BACKLOG_CAPACITY);

    Thread[] threads = new Thread[NTHREADS];
    AtomicBoolean run = new AtomicBoolean(true);
    for (int i = 0; i < NTHREADS; ++i) {
      threads[i] = new Thread(() -> {
        while (run.get() || !jobs.isEmpty()) {
          IOConsumer<Void> job;
          try {
            job = jobs.poll(1, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            break;
          }
          if (job == null) {
            continue;
          }
          try {
            job.accept(null);
          } catch (InterruptedIOException ignored) {
            break;
          } catch (IOException e) {
            onError(e);
          }
        }
      });
      threads[i].setName(backupTarget.name().toString() + "-worker-" + i);
      threads[i].start();
    }

    return new BackupTarget() {
      @Override
      public Id name() {
        return backupTarget.name();
      }

      @Override
      public void backup(Resource r, IOConsumer<Id> k) throws IOException {
        try {
          jobs.put((v) -> backupTarget.backup(r, k));
        } catch (InterruptedException e) {
          throw new IOException(e);
        }
      }

      @Override
      public void close() throws Exception {
        System.out.println("joining worker threads");
        run.set(false);
        for (Thread t : threads) {
          try {
            t.join();
          } catch (InterruptedException ignored) {
          }
        }
        System.out.println("done!");
        backupTarget.close();
      }
    };
  }

  private static void onError(Exception e) {
    numErrs.incrementAndGet();
    e.printStackTrace();
  }

  private static abstract class Visitor implements FileVisitor<Path> {
    private List<PathMatcher> exclusionRules;

    public Visitor(List<PathMatcher> exclusionRules) {
      this.exclusionRules = exclusionRules;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      return exclusionRules.stream().anyMatch(r -> r.matches(dir) || r.matches(dir.getFileName())) ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      if (exclusionRules.stream().allMatch(r -> !r.matches(file) && !r.matches(file.getFileName()))) {
        if (attrs.isSymbolicLink()) {
          Path dst = Files.readSymbolicLink(file);
          onSymLink(file, dst);
        } else {
          onFile(file);
        }
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      System.err.println("failed to visit: " + file);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      return FileVisitResult.CONTINUE;
    }

    protected abstract void onFile(Path file) throws IOException;
    protected abstract void onSymLink(Path src, Path dst) throws IOException;
  }

  private static void forEachFile(IOConsumer<SymLink> symlinkConsumer, IOConsumer<Resource> consumer) throws IOException {
    Collection<Path> paths = new LinkedHashSet<>();

    Pattern p = Pattern.compile("^(.) (.*)$");
    for (int i = 0; i < RULES.size(); ++i) {
      String r = RULES.get(i);
      Matcher m = p.matcher(r);
      if (m.find()) {
        char c = m.group(1).charAt(0);
        String rule = m.group(2);
        switch (c) {
          case '+':
            rule = rule.replace("~", System.getProperty("user.home"));
            Files.walkFileTree(Paths.get(rule), new Visitor(
                RULES.subList(i + 1, RULES.size()).stream()
                    .filter(rr -> rr.startsWith("-"))
                    .map(rr -> { Matcher mm = p.matcher(rr); return mm.find() ? FileSystems.getDefault().getPathMatcher("glob:" + mm.group(2)) : Util.fail(); })
                    .collect(Collectors.toList())) {
              @Override
              protected void onFile(Path path) throws IOException {
                if (paths.add(path)) {
                  consumer.accept(new Resource() {
                    @Override
                    public Id system() {
                      return SYSTEM_ID;
                    }

                    @Override
                    public Path path() {
                      return path;
                    }
                    @Override
                    public Instant modTime() throws IOException {
                      return Files.getLastModifiedTime(path).toInstant();
                    }

                    @Override
                    public InputStream open() throws IOException {
                      return new FileInputStream(path().toString());
                    }
                  });
                }
              }

              @Override
              protected void onSymLink(Path src, Path dst) throws IOException {
                if (paths.add(src)) {
                  symlinkConsumer.accept(new SymLink() {
                    @Override
                    public Path src() {
                      return src;
                    }

                    @Override
                    public Path dst() {
                      return dst;
                    }
                  });
                }
              }
            });
            break;
          case '-':
            break;
          default:
            throw new RuntimeException("unknown start char " + c);
        }
      }
    }
  }

}
