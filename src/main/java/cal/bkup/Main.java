package cal.bkup;

import cal.bkup.impls.CachedDirectory;
import cal.bkup.impls.EncryptedDirectory;
import cal.bkup.impls.EncryptedInputStream;
import cal.bkup.impls.FilesystemBackupTarget;
import cal.bkup.impls.FreeOp;
import cal.bkup.impls.GlacierBackupTarget;
import cal.bkup.impls.LocalDirectory;
import cal.bkup.impls.S3Directory;
import cal.bkup.impls.SqliteCheckpoint;
import cal.bkup.impls.XZCompressedDirectory;
import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.Config;
import cal.bkup.types.HardLink;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Price;
import cal.bkup.types.Resource;
import cal.bkup.types.ResourceInfo;
import cal.bkup.types.Rule;
import cal.bkup.types.SimpleDirectory;
import cal.bkup.types.SymLink;
import com.fasterxml.jackson.databind.ObjectMapper;
import jnr.posix.POSIX;
import jnr.posix.POSIXFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {

  private static final String AWS_REGION = "us-east-2";
  private static final String GLACIER_VAULT_NAME = "mybackups";
  private static final String GLACIER_ENDPOINT = "glacier." + AWS_REGION + ".amazonaws.com";
  private static final String S3_BUCKET = "backupindex";
  private static final String S3_ENDPOINT = "s3." + AWS_REGION + ".amazonaws.com";
  private static final int BACKLOG_CAPACITY = 8;
  private static final int NTHREADS = Runtime.getRuntime().availableProcessors();
  private static final String HOME = System.getProperty("user.home");
  private static final Path CFG_FILE = Paths.get(HOME, ".backup-config.json").toAbsolutePath();

  private static final AtomicLong numBackedUp = new AtomicLong(0);
  private static final AtomicLong numSkipped = new AtomicLong(0);
  private static final AtomicLong numErrs = new AtomicLong(0);

  private static void showHelp(Options options) {
    new HelpFormatter().printHelp("backup [options]", options);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("h", "help", false, "Show help and quit");
    options.addOption("p", "password", true, "Encryption password");
    options.addOption("b", "backup", false, "Back up files");
    options.addOption("L", "local", false, "Local backup to /tmp (for testing)");
    options.addOption("d", "dry-run", false, "Show what would be done, but do nothing");
    options.addOption("l", "list", false, "Show inventory of current backup");
    options.addOption(Option.builder().longOpt("gc").desc("Delete old/unused backups").build());

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

    final boolean dryRun = cli.hasOption('d');
    final boolean list = cli.hasOption('l');
    final boolean gc = cli.hasOption("gc");
    final boolean backup = cli.hasOption("backup");
    final boolean local = cli.hasOption("local");
    final String password = cli.hasOption('p') ? cli.getOptionValue('p') : Util.readPassword();

    final Config config;
    try {
      config = loadConfig(CFG_FILE);
    } catch (FileNotFoundException e) {
      System.err.println("Config file '" + CFG_FILE + "' not found");
      System.exit(1);
      return;
    }

    if (password != null) {
      if (list) {
        try (Checkpoint checkpoint = findMostRecentCheckpoint(password, local)) {
          checkpoint.list().forEach(info -> {
            System.out.println("/" + info.system() + info.path() + " [" + info.target() + '/' + info.idAtTarget() + '/' + info.modTime() + ']');
          });
          checkpoint.symlinks(config.systemName()).forEach(link -> {
            System.out.println("/" + link.src() + " [SOFT] ----> " + link.dst());
          });
          checkpoint.hardlinks(config.systemName()).forEach(link -> {
            System.out.println("/" + link.src() + " [HARD] ----> " + link.dst());
          });
        }
      }

      try (Checkpoint checkpoint = findMostRecentCheckpoint(password, local);
           BackupTarget target = getBackupTarget(password, local)) {

        System.out.println("Planning...");
        List<Op<?>> plan = new ArrayList<>();
        if (backup) {
          plan.addAll(planBackup(config, checkpoint, target).collect(Collectors.toList()));
        }
        if (gc) {
          Set<Id> ids = checkpoint.list()
              .filter(info -> info.target().equals(target.name()))
              .map(ResourceInfo::idAtTarget)
              .collect(Collectors.toSet());
          target.list().forEach(x -> {
            if (!ids.contains(x.idAtTarget())) {
              try {
                plan.add(target.delete(x));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          });
        }

        System.out.println("Estimating costs...");
        long bytesXferred = plan.stream().mapToLong(Op::estimatedDataTransfer).sum();
        Price cost = plan.stream().map(Op::cost).reduce(Price.ZERO, Price::plus);
        Price maint = plan.stream().map(Op::monthlyMaintenanceCost).reduce(Price.ZERO, Price::plus);

        System.out.println("Execution plan:");
        System.out.println("    #ops: " + plan.size());
        System.out.println("    xfer: ~" + Util.divideAndRoundUp(bytesXferred, 1024 * 1024) + " Mb");
        System.out.println("    cost: ~" + Util.formatPrice(cost));
        System.out.println("    maint: ~" + Util.formatPrice(maint));
        System.out.println("-----------------------------------------");

        if (!dryRun) {
          try {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(BACKLOG_CAPACITY);
            ExecutorService executor = new ThreadPoolExecutor(
                NTHREADS, NTHREADS,
                Long.MAX_VALUE, TimeUnit.SECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy());

            AtomicLong done = new AtomicLong(0);

            Instant start = Instant.now();
            for (Op<?> op : plan) {
              System.out.println("[" + String.format("%2d", done.get() * 100 / plan.size()) + "%] starting " + op);
              executor.execute(() -> {
                try {
                  op.exec();
                  long ndone = done.incrementAndGet();
                  System.out.println("[" + String.format("%2d", ndone * 100 / plan.size()) + "%] finished " + op);
                  numBackedUp.incrementAndGet();
                  synchronized (checkpoint) {
                    Instant lastSave = checkpoint.lastSave();
                    if (lastSave == null || max(lastSave, start).isBefore(Instant.now().minus(5, ChronoUnit.MINUTES))) {
                      checkpoint.save();
                    }
                  }
                } catch (Exception e) {
                  onError(e);
                }
              });
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
          } finally {
            long nbkup = numBackedUp.get();
            System.out.println(nbkup + " ops, " + numSkipped + " unchanged files, " + numErrs + " errors");
            if (plan.size() > 0) {
              checkpoint.save();
            }
          }
        } else {
          for (Op<?> o : plan) {
            System.out.println(o);
          }
        }
      }
    } else {
      throw new Exception("failed to get password");
    }
  }

  private static class RawConfig {
    public String system;
    public List<String> rules;
  }

  private static Config loadConfig(Path target) throws IOException {

    RawConfig r;
    try (InputStream in = new FileInputStream(target.toString())) {
      r = new ObjectMapper().readValue(in, RawConfig.class);
    }

    Id systemId = new Id(r.system);
    Pattern p = Pattern.compile("^(.) (.*)$");
    List<Rule> rules = new ArrayList<>();
    for (String rule : r.rules) {
      Matcher m = p.matcher(rule);
      if (m.find()) {
        char c = m.group(1).charAt(0);
        String ruleText = m.group(2);
        if (ruleText.startsWith("~")) {
          ruleText = ruleText.replaceFirst(Pattern.quote("~"), HOME);
        }
        String finalRuleText = ruleText;
        switch (c) {
          case '+':
            rules.add((include, exclude) -> include.accept(Paths.get(finalRuleText)));
            break;
          case '-':
            rules.add((include, exclude) -> exclude.accept(FileSystems.getDefault().getPathMatcher("glob:" + finalRuleText)));
            break;
          default:
            throw new IllegalArgumentException("Cannot process rule '" + rule + '\'');
        }
      } else {
        throw new IllegalArgumentException("Cannot process rule '" + rule + '\'');
      }
    }

    return new Config() {
      @Override
      public Id systemName() {
        return systemId;
      }

      @Override
      public List<Rule> backupRules() {
        return rules;
      }
    };
  }

  private static <T extends Comparable<T>> T max(T x, T y) {
    return x.compareTo(y) < 0 ? y : x;
  }

  private static Stream<Op<?>> planBackup(Config config, Checkpoint checkpoint, BackupTarget target) throws IOException {
    Set<Path> presentSymlinks = new HashSet<>();
    Set<Path> presentHardlinks = new HashSet<>();
    Set<Path> presentFiles = new HashSet<>();

    Map<Path, Path> checkpointedSymlinks = checkpoint.symlinks(config.systemName())
        .collect(Collectors.toMap(SymLink::src, SymLink::dst));
    Map<Path, Path> checkpointedHardlinks = checkpoint.hardlinks(config.systemName())
        .collect(Collectors.toMap(HardLink::src, HardLink::dst));
    Set<Path> checkpointedFiles = checkpoint.list()
        .filter(r -> r.system().equals(config.systemName()))
        .map(ResourceInfo::path)
        .collect(Collectors.toCollection(HashSet::new));

    List<Op<?>> ops = new ArrayList<>();

    // Add new things
    forEachFile(
        config,
        symlink -> {
          presentSymlinks.add(symlink.src());
          if (!Objects.equals(symlink.dst(), checkpointedSymlinks.get(symlink.src()))) {
            ops.add(new FreeOp<Void>() {
              @Override
              public Void exec() throws IOException {
                checkpoint.noteSymLink(config.systemName(), symlink);
                return null;
              }

              @Override
              public String toString() {
                return "MAKE SOFT LINK " + symlink.src() + " ---> " + symlink.dst();
              }
            });
          } else {
            numSkipped.incrementAndGet();
          }
        },
        hardlink -> {
          presentHardlinks.add(hardlink.src());
          if (!Objects.equals(hardlink.dst(), checkpointedHardlinks.get(hardlink.src()))) {
            ops.add(new FreeOp<Void>() {
              @Override
              public Void exec() throws IOException {
                checkpoint.noteHardLink(config.systemName(), hardlink);
                return null;
              }

              @Override
              public String toString() {
                return "MAKE HARD LINK " + hardlink.src() + " ---> " + hardlink.dst();
              }
            });
          } else {
            numSkipped.incrementAndGet();
          }
        },
        resource -> {
          presentFiles.add(resource.path());
          Instant checkpointModTime = checkpoint.modTime(resource, target);
          if (checkpointModTime == null || resource.modTime().compareTo(checkpointModTime) > 0) {
            Op<Id> op = target.backup(resource);
            ops.add(new Op<Void>() {

              @Override
              public Price cost() {
                return op.cost();
              }

              @Override
              public Price monthlyMaintenanceCost() {
                return op.monthlyMaintenanceCost();
              }

              @Override
              public long estimatedDataTransfer() {
                return op.estimatedDataTransfer();
              }

              @Override
              public Void exec() throws IOException {
                checkpoint.noteSuccessfulBackup(resource, target, op.exec());
                return null;
              }

              @Override
              public String toString() {
                return op.toString();
              }
            });
          } else {
            numSkipped.incrementAndGet();
          }
        });

    // Delete old things
    Set<Path> symLinksToRemove = new HashSet<>(checkpointedSymlinks.keySet());
    Set<Path> hardLinksToRemove = new HashSet<>(checkpointedHardlinks.keySet());
    Set<Path> filesToRemove = new HashSet<>(checkpointedFiles);

    symLinksToRemove.removeAll(presentSymlinks);
    hardLinksToRemove.removeAll(presentHardlinks);
    filesToRemove.removeAll(presentFiles);

    for (Path p : filesToRemove) {
      ops.add(new FreeOp<Void>() {
        @Override
        public Void exec() throws IOException {
          checkpoint.forgetBackup(config.systemName(), p);
          return null;
        }

        @Override
        public String toString() {
          return "forget backed up file " + p.toString();
        }
      });
    }

    for (Path p : symLinksToRemove) {
      ops.add(new FreeOp<Void>() {
        @Override
        public Void exec() throws IOException {
          checkpoint.forgetSymLink(config.systemName(), p);
          return null;
        }

        @Override
        public String toString() {
          return "forget soft link " + p.toString();
        }
      });
    }

    for (Path p : hardLinksToRemove) {
      ops.add(new FreeOp<Void>() {
        @Override
        public Void exec() throws IOException {
          checkpoint.forgetHardLink(config.systemName(), p);
          return null;
        }

        @Override
        public String toString() {
          return "forget hard link " + p.toString();
        }
      });
    }

    return ops.stream().filter(Objects::nonNull);
  }

  private static Checkpoint findMostRecentCheckpoint(String password, boolean local) throws IOException {
    System.out.println("Fetching checkpoint...");
    try {
      Path cacheLoc = Paths.get("/tmp/s3cache");
      SimpleDirectory dir = local ?
          new XZCompressedDirectory(new EncryptedDirectory(LocalDirectory.TMP, password)) :
          new XZCompressedDirectory(new EncryptedDirectory(new CachedDirectory(new S3Directory(S3_BUCKET, S3_ENDPOINT), cacheLoc), password));
      return new SqliteCheckpoint(dir);
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private static BackupTarget getBackupTarget(String password, boolean local) throws GeneralSecurityException, UnsupportedEncodingException {
    BackupTarget baseTarget = local ?
        new FilesystemBackupTarget(Paths.get("/tmp/bkup")) :
        new GlacierBackupTarget(GLACIER_ENDPOINT, GLACIER_VAULT_NAME);
    return encryptTarget(baseTarget, password);
  }

  private static BackupTarget encryptTarget(BackupTarget backupTarget, String password) throws GeneralSecurityException, UnsupportedEncodingException {
    return new BackupTarget() {
      @Override
      public Id name() {
        return backupTarget.name();
      }

      @Override
      public Op<Id> backup(Resource r) throws IOException {
        return backupTarget.backup(new Resource() {
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

          @Override
          public long sizeEstimateInBytes() throws IOException {
            // TODO: can we do better?
            return r.sizeEstimateInBytes();
          }
        });
      }

      @Override
      public Stream<BackedUpResourceInfo> list() throws IOException {
        return backupTarget.list();
      }

      @Override
      public Op<Void> delete(BackedUpResourceInfo id) throws IOException {
        return backupTarget.delete(id);
      }

      @Override
      public void close() throws Exception {
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
          onFile(file, attrs);
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

    protected abstract void onFile(Path file, BasicFileAttributes attrs) throws IOException;
    protected abstract void onSymLink(Path src, Path dst) throws IOException;
  }

  private static void forEachFile(Config config, IOConsumer<SymLink> symlinkConsumer, IOConsumer<HardLink> hardLinkConsumer, IOConsumer<Resource> consumer) throws IOException {
    Collection<Path> paths = new HashSet<>();

    // TODO: this will work best if we have a deterministic exploration order :/
    Map<Long, Path> inodes = new HashMap<>();
    List<Rule> rules = config.backupRules();

    POSIX posix = POSIXFactory.getPOSIX();
    assert posix.isNative();

    Pattern p = Pattern.compile("^(.) (.*)$");
    for (int i = 0; i < rules.size(); ++i) {
      Rule r = rules.get(i);

      int index = i;
      r.destruct(
          include -> {
            List<PathMatcher> exclusions = new ArrayList<>();
            rules.subList(index + 1, rules.size())
                .forEach(rr -> {
                  try {
                    r.destruct(inc -> {}, exclusions::add);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                });
            Files.walkFileTree(include, new Visitor(exclusions) {
              @Override
              protected void onFile(Path path, BasicFileAttributes attrs) throws IOException {
                if (paths.add(path)) {
                  long inode = posix.stat(path.toString()).ino();
                  Path preexisting = inodes.get(inode);

                  if (preexisting != null) {
                    hardLinkConsumer.accept(new HardLink() {
                      @Override
                      public Path src() {
                        return path;
                      }

                      @Override
                      public Path dst() {
                        return preexisting;
                      }
                    });
                  } else {
                    inodes.put(inode, path);
                    long size = attrs.size();
                    consumer.accept(new Resource() {
                      @Override
                      public Id system() {
                        return config.systemName();
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

                      @Override
                      public long sizeEstimateInBytes() throws IOException {
                        return size;
                      }
                    });
                  }
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
          },
          exclude -> { });
    }
  }

}
