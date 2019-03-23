package cal.bkup;

import cal.prim.ConsistentBlob;
import cal.prim.NoValue;
import cal.prim.transforms.CachedDirectory;
import cal.prim.TimestampedBlobInDirectory;
import cal.bkup.impls.EncryptedBackupTarget;
import cal.bkup.impls.FilesystemBackupTarget;
import cal.bkup.impls.FreeOp;
import cal.bkup.impls.GlacierBackupTarget;
import cal.prim.LocalDirectory;
import cal.bkup.impls.ProgressDisplay;
import cal.bkup.impls.SqliteCheckpoint;
import cal.bkup.impls.SqliteCheckpoint2;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Checkpoint;
import cal.bkup.types.CheckpointFormat;
import cal.bkup.types.Config;
import cal.bkup.types.HardLink;
import cal.bkup.types.Id;
import cal.bkup.types.IncorrectFormatException;
import cal.bkup.types.Op;
import cal.prim.Price;
import cal.bkup.types.ResourceInfo;
import cal.bkup.types.Rule;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.SymLink;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.S3Directory;
import cal.prim.transforms.Encryption;
import cal.prim.transforms.StatisticsCollectingInputStream;
import cal.prim.transforms.TransformedDirectory;
import cal.prim.transforms.XZCompression;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedInputStream;
import java.io.Console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
  private static final String DYNAMO_TABLE = "backupconsistency";
  private static final String DYNAMO_REGISTER = "clock";
  private static final int BACKLOG_CAPACITY = 8;
  private static final int NTHREADS = Runtime.getRuntime().availableProcessors();
  private static final String HOME = System.getProperty("user.home");
  private static final Path CFG_FILE = Paths.get(HOME, ".backup-config.json").toAbsolutePath();

  private static final AtomicLong numSuccessful = new AtomicLong(0);
  private static final AtomicLong numSkipped = new AtomicLong(0);
  private static final AtomicLong numErrs = new AtomicLong(0);

  private static void showHelp(Options options) {
    new HelpFormatter().printHelp("backup [options]", options);
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    // flags
    options.addOption("h", "help", false, "Show help and quit");
    options.addOption("p", "password", true, "Encryption password");
    options.addOption("L", "local", false, "Local backup to /tmp (for testing)");
    options.addOption("d", "dry-run", false, "Show what would be done, but do nothing");

    // actions
    options.addOption("b", "backup", false, "Back up files");
    options.addOption("l", "list", false, "Show inventory of current backup");
    options.addOption("c", "spot-check", true, "Spot-check N backed-up files");
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
    final int numToCheck = cli.hasOption('c') ? Integer.parseInt(cli.getOptionValue('c')) : 0;
    final String password = cli.hasOption('p') ? cli.getOptionValue('p') : Util.readPassword();

    final Config config;
    try {
      config = loadConfig(CFG_FILE);
    } catch (FileNotFoundException e) {
      System.err.println("Config file '" + CFG_FILE + "' not found");
      System.exit(1);
      return;
    }

    AtomicBoolean success = new AtomicBoolean(true);

    if (password != null) {
      final ConsistentBlob checkpointStore = checkpointStore(password, local);
      final AtomicReference<ConsistentBlob.Tag> token = new AtomicReference<>(checkpointStore.head());

      if (list) {
        try (Checkpoint checkpoint = loadCheckpoint(checkpointStore, token.get())) {
          checkpoint.list().forEach(info -> {
            System.out.println("/" + info.system() + info.path() + " [" + info.target() + '|' + info.idAtTarget() + '|' + info.modTime() + ']');
          });
          checkpoint.symlinks(config.systemName()).forEach(link -> {
            System.out.println("/" + link.src() + " [SOFT] ----> " + link.dst());
          });
          checkpoint.hardlinks(config.systemName()).forEach(link -> {
            System.out.println("/" + link.src() + " [HARD] ----> " + link.dst());
          });
        }
      }

      try (Checkpoint checkpoint = loadCheckpoint(checkpointStore, token.get());
           BackupTarget target = getBackupTarget(password, local)) {

        System.out.println("Planning...");
        List<Op<?>> plan = new ArrayList<>();
        if (backup) {
          plan.addAll(planBackup(config, checkpoint, target).collect(Collectors.toList()));
        }
        if (gc) {
          // TODO: clean up checkpoints too
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

        if (numToCheck > 0) {
          List<ResourceInfo> infos = checkpoint.list()
              .filter(i -> i.system().equals(config.systemName()))
              .filter(i -> {
                    try {
                      return Files.exists(i.path()) && Util.le(Files.getLastModifiedTime(i.path()).toInstant(), i.modTime());
                    } catch (IOException e) {
                      return false;
                    }
                  }
              )
              .collect(Collectors.toCollection(ArrayList::new));
          Random r = new Random();
          Collections.shuffle(infos, r);
          infos = infos.subList(0, Math.min(infos.size(), numToCheck));
          plan.add(target.fetch(infos, res -> {
            ResourceInfo i = res.fst;
            System.out.println("Checking " + i.path() + "...");
            byte[] remoteSha;
            try (InputStream in = res.snd) {
              remoteSha = Util.sha256(in);
            }
            byte[] localSha;
            try (InputStream in = new FileInputStream(i.path().toFile())) {
              localSha = Util.sha256(in);
            }
            if (Arrays.equals(remoteSha, localSha)) {
              System.out.println("OK!");
            } else {
              System.out.println("FAILURE");
              System.out.println("  Local  SHA256: " + Arrays.toString(localSha));
              System.out.println("  Remote SHA256: " + Arrays.toString(remoteSha));
              success.set(false);
            }
          }));
        }

        System.out.println("Estimating costs...");
        long bytesXferred = plan.stream().mapToLong(Op::estimatedDataTransfer).sum();
        Price cost = plan.stream().map(Op::cost).reduce(Price.ZERO, Price::plus);
        Price maint = plan.stream().map(Op::monthlyMaintenanceCost).reduce(Price.ZERO, Price::plus);

        for (Op<?> op : plan) {
          System.out.println(op);
        }

        System.out.println("-----------------------------------------");
        System.out.println("Execution plan:");
        System.out.println("    #ops: " + plan.size());
        System.out.println("    xfer: ~" + Util.divideAndRoundUp(bytesXferred, 1024 * 1024) + " Mb");
        System.out.println("    cost: ~" + Util.formatPrice(cost));
        System.out.println("    maint: ~" + Util.formatPrice(maint));
        System.out.println("-----------------------------------------");

        if (!Objects.equals(cost.plus(maint), Price.ZERO)) {
          if (!confirm("Proceed?")) {
            return;
          }
        }

        if (!dryRun) {
          try {
            BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(BACKLOG_CAPACITY);
            ExecutorService executor = new ThreadPoolExecutor(
                NTHREADS, NTHREADS,
                Long.MAX_VALUE, TimeUnit.SECONDS,
                queue,
                new ThreadPoolExecutor.CallerRunsPolicy());

            Instant start = Instant.now();
            AtomicReference<Instant> lastSaveRef = new AtomicReference<>(start);

            try (ProgressDisplay display = new ProgressDisplay(plan.size())) {
              for (Op<?> op : plan) {
                final ProgressDisplay.Task t = display.startTask(op.toString());
                executor.execute(() -> {
                  try {
                    op.exec((numerator, denominator) -> display.reportProgress(t, numerator, denominator));
                    display.finishTask(t);
                    numSuccessful.incrementAndGet();
                    if (backup) {
                      Instant now = Instant.now();
                      Instant lastSave = lastSaveRef.get();
                      synchronized (checkpoint) {
                        if (max(lastSave, start).isBefore(now.minus(5, ChronoUnit.MINUTES))) {
                          token.set(checkpointStore.write(token.get(), Util.createInputStream(out -> {
                            System.out.println("Saving checkpoint [" + now.toEpochMilli() + ']');
                            checkpoint.save(out);
                            System.out.println("Checkpointed!");
                          })));
                          lastSaveRef.set(now);
                        }
                      }
                    }
                  } catch (Exception e) {
                    onError(e);
                  }
                });
              }

              executor.shutdown();
              executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            }
          } finally {
            long nbkup = numSuccessful.get();
            System.out.println(nbkup + " ops, " + numSkipped + " unchanged files, " + numErrs + " errors");
            if (backup) {
              Instant now = Instant.now();
              token.set(checkpointStore.write(token.get(), Util.createInputStream(out -> {
                System.out.println("Saving checkpoint [" + now.toEpochMilli() + ']');
                checkpoint.save(out);
                System.out.println("Checkpointed!");
              })));
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
    System.exit(success.get() ? 0 : 1);
  }

  private static boolean confirm(String prompt) {
    Console cons = System.console();
    if (cons == null) {
      return false;
    }
    String input = cons.readLine("%s [y/n] ", prompt);
    return Character.toLowerCase(input.charAt(0)) == 'y';
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

  private static Stream<Op<Void>> planBackup(Config config, Checkpoint checkpoint, BackupTarget target) throws IOException {
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

    List<Op<Void>> ops = new ArrayList<>();

    // Add new things
    FileTools.forEachFile(
        config,
        symlink -> {
          presentSymlinks.add(symlink.src());
          if (!Objects.equals(symlink.dst(), checkpointedSymlinks.get(symlink.src()))) {
            ops.add(new FreeOp<Void>() {
              @Override
              public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
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
              public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
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
          Instant resourceModTime = resource.modTime();
          if (checkpointModTime == null || !Objects.equals(checkpointModTime, resourceModTime)) {
            long estimatedSize = resource.sizeEstimateInBytes();
            ops.add(new Op<Void>() {
              @Override
              public Price cost() {
                return target.estimatedCostOfDataTransfer(estimatedSize);
              }

              @Override
              public Price monthlyMaintenanceCost() {
                return target.estimatedMonthlyMaintenanceCost(estimatedSize);
              }

              @Override
              public long estimatedDataTransfer() {
                return estimatedSize;
              }

              @Override
              public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
                // compute sha256 and size
                StatisticsCollectingInputStream stream = new StatisticsCollectingInputStream(resource.open(), s -> progressCallback.reportProgress(s.getBytesRead(), estimatedSize * 2));
                try (InputStream in = new BufferedInputStream(stream, Util.SUGGESTED_BUFFER_SIZE)) {
                  int x;
                  do {
                    x = in.read();
                  } while (x >= 0);
                }
                byte[] originalSha256 = stream.getSha256Digest();
                long originalSize = stream.getBytesRead();

                // determine whether the data is already backed up
                BackupReport report = checkpoint.findBlob(target, originalSha256, originalSize);
                if (report == null) {
                  // if the data is not already backed up, back it up!
                  stream = new StatisticsCollectingInputStream(resource.open(), s -> progressCallback.reportProgress(originalSize + s.getBytesRead(), estimatedSize * 2));
                  try (InputStream toClose = new BufferedInputStream(stream, Util.SUGGESTED_BUFFER_SIZE)) {
                    report = target.backup(toClose, estimatedSize);
                  }
                  assert stream.isClosed();

                  // TODO: abort!
                  if (!Arrays.equals(originalSha256, stream.getSha256Digest()) || originalSize != stream.getBytesRead()) {
                    throw new ConcurrentModificationException(resource + " was modified during backup");
                  }
                }

                byte[] finalSha256 = stream.getSha256Digest();
                long finalSize = stream.getBytesRead();

                checkpoint.noteSuccessfulBackup(target.name(), resource, new Sha256AndSize() {
                  @Override
                  public byte[] sha256() {
                    return finalSha256;
                  }

                  @Override
                  public long size() {
                    return finalSize;
                  }
                }, report);
                return null;
              }

              @Override
              public String toString() {
                String startTime = checkpointModTime == null ? "missing" : checkpointModTime.toString();
                return resource.path().toString() + " [" + startTime + " --> " + resourceModTime + ']';
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
        public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
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
        public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
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
        public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
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

  private static EventuallyConsistentDirectory checkpointDir(String password, boolean local) throws IOException {
    Path cacheLoc = Paths.get("/tmp/s3cache");
    EventuallyConsistentDirectory rawDir = local ?
        LocalDirectory.TMP :
        new CachedDirectory(new S3Directory(AmazonS3ClientBuilder
                        .standard()
                        .withCredentials(AWSTools.credentialsProvider())
                        .withRegion(AWS_REGION)
                        .build(), S3_BUCKET), cacheLoc);
    return new TransformedDirectory(rawDir, new XZCompression(), new Encryption(password));
  }

  private static ConsistentBlob checkpointStore(String password, boolean local) throws IOException {
    return new TimestampedBlobInDirectory(checkpointDir(password, local));
  }

  /**
   * Contains at least one element.
   * Preferred format first.
   */
  private final static CheckpointFormat[] FORMATS = {
          SqliteCheckpoint2.FORMAT,
          SqliteCheckpoint.FORMAT,
  };

  private static Checkpoint loadCheckpoint(ConsistentBlob store, ConsistentBlob.Tag token) throws IOException {
    System.out.println("Fetching checkpoint...");
    for (CheckpointFormat format : FORMATS) {
      Checkpoint result;
      try (InputStream in = store.read(token)) {
        result = format.tryRead(in);
      } catch (IncorrectFormatException e) {
        System.out.println("Does not match format " + format.name() + ": " + e.getMessage());
        continue;
      } catch (NoValue noValue) {
        System.out.println("No index was found; creating a new one");
        return FORMATS[0].createEmpty();
      }
      System.out.println("Loaded checkpoint " + token + " (format=" + format.name() + ')');
      if (format != FORMATS[0]) {
        System.out.println("Migrating from " + format.name() + " to " + FORMATS[0].name());
        return FORMATS[0].migrateFrom(result);
      }
      return result;
    }
    throw new IOException("Checkpoint " + token + " does not match any known format");
  }

  private static BackupTarget getBackupTarget(String password, boolean local) throws GeneralSecurityException, UnsupportedEncodingException {
    BackupTarget baseTarget = local ?
        new FilesystemBackupTarget(Paths.get("/tmp/bkup")) :
        new GlacierBackupTarget(GLACIER_ENDPOINT, GLACIER_VAULT_NAME);
    return encryptTarget(baseTarget, password);
  }

  private static BackupTarget encryptTarget(BackupTarget backupTarget, String password) throws GeneralSecurityException, UnsupportedEncodingException {
    return new EncryptedBackupTarget(backupTarget, password);
  }

  private static void onError(Exception e) {
    numErrs.incrementAndGet();
    e.printStackTrace();
  }

}
