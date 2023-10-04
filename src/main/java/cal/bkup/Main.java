package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormatV01;
import cal.bkup.impls.JsonIndexFormatV02;
import cal.bkup.impls.JsonIndexFormatV03;
import cal.bkup.impls.JsonIndexFormatV04;
import cal.bkup.impls.ProgressDisplay;
import cal.bkup.impls.VersionedIndexFormat;
import cal.bkup.types.Config;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.Rule;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.SystemId;
import cal.prim.NoValue;
import cal.prim.Pair;
import cal.prim.PreconditionFailed;
import cal.prim.Price;
import cal.prim.concurrency.DynamoDBStringRegister;
import cal.prim.concurrency.SQLiteStringRegister;
import cal.prim.concurrency.StringRegister;
import cal.prim.fs.Filesystem;
import cal.prim.fs.HardLink;
import cal.prim.fs.PhysicalFilesystem;
import cal.prim.fs.RegularFile;
import cal.prim.fs.SymLink;
import cal.prim.storage.BlobStoreOnDirectory;
import cal.prim.storage.ConsistentBlob;
import cal.prim.storage.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.storage.EventuallyConsistentBlobStore;
import cal.prim.storage.EventuallyConsistentDirectory;
import cal.prim.storage.GlacierBlobStore;
import cal.prim.storage.LocalDirectory;
import cal.prim.storage.S3Directory;
import cal.prim.time.UnreliableWallClock;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.XZCompression;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.fraction.BigFraction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.BufferedInputStream;
import java.io.Console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {

  private static final Region AWS_REGION = Region.US_EAST_2;
  private static final String GLACIER_VAULT_NAME = "blobs";
  private static final String S3_BUCKET = "backupindex";
  private static final String DYNAMO_TABLE = "backupconsistency";
  private static final String DYNAMO_REGISTER = "clock";
  private static final int NTHREADS = Runtime.getRuntime().availableProcessors();
  private static final String HOME = System.getProperty("user.home");
  private static final Path CFG_FILE = Paths.get(HOME, ".backup-config.json").toAbsolutePath();

  // See https://aws.amazon.com/glacier/pricing/
  // See https://aws.amazon.com/glacier/faqs/
  private static final BigFraction PENNIES_PER_UPLOAD_REQ = new BigFraction(0.05).multiply(new BigFraction(100)).divide(new BigFraction(1000));
  private static final BigFraction PENNIES_PER_GB_MONTH = new BigFraction(0.004).multiply(new BigFraction(100));
  private static final BigFraction EXTRA_BYTES_PER_ARCHIVE = new BigFraction(32 * 1024); // 32 Kb
  private static final BigFraction ONE_GB = new BigFraction(1024L * 1024L * 1024L);
  private static final Duration ONE_MONTH = Duration.of(30, ChronoUnit.DAYS);
  private static final Duration MINIMUM_STORAGE_DURATION = ONE_MONTH.multipliedBy(3);
  private static final BigFraction EARLY_DELETION_FEE_PER_GB_MONTH = PENNIES_PER_GB_MONTH;
  private static final BigFraction PENNIES_PER_DOWNLOAD_REQ = BigFraction.ZERO;
  private static final BigFraction PENNIES_PER_DOWNLOAD_GB_REQ = BigFraction.ONE;
  private static final BigFraction PENNIES_PER_DOWNLOAD_GB_XFER = new BigFraction(9); // assuming <10TB total transfer per month
  private static final BigFraction MONTHS_PER_SECOND = BigFraction.ONE.divide(new BigFraction(ONE_MONTH.get(ChronoUnit.SECONDS)));
  private static final StorageCostModel COST_MODEL = new StorageCostModel() {
    @Override
    public Price costToUploadBlob(long numBytes) {
      return numBytes <= AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK ?
              new Price(PENNIES_PER_UPLOAD_REQ) :
              new Price(PENNIES_PER_UPLOAD_REQ.multiply(new BigFraction(numBytes / AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK).add(BigFraction.TWO)));
    }

    @Override
    public Price costToDeleteBlob(long numBytes, Duration timeSinceUpload) {
      // Delete requests are free, but "archives deleted before 90 days incur a pro-rated charge equal to the storage
      // charge for the remaining days".  https://aws.amazon.com/glacier/pricing/
      Duration remaining = MINIMUM_STORAGE_DURATION.minus(timeSinceUpload);
      if (remaining.isNegative()) {
        return Price.ZERO;
      }
      long seconds = remaining.toSeconds();
      long secondsPerMonth = 2592000;
      return new Price(monthlyStorageCostForBlob(numBytes).valueInCents().divide(secondsPerMonth).multiply(seconds));
    }

    @Override
    public Price monthlyStorageCostForBlob(long numBytes) {
      return new Price(PENNIES_PER_GB_MONTH.multiply(new BigFraction(numBytes).add(EXTRA_BYTES_PER_ARCHIVE)).divide(ONE_GB));
    }
  };

  private static void showHelp(Options options) {
    new HelpFormatter().printHelp("backup [options]", options);
  }

  public static void main(String[] args) throws IOException {
    Options options = new Options();

    // flags
    options.addOption("h", "help", false, "Show help and quit");
    options.addOption("p", "password", true, "Decryption password");
    options.addOption("P", "new-password", true, "Encryption password (defaults to the decryption password; prompts if you give an empty argument)");
    options.addOption("L", "local", false, "Local backup to /tmp (for testing)");
    options.addOption("d", "dry-run", false, "Show what would be done, but do nothing");

    // actions
    options.addOption("b", "backup", false, "Back up files");
    options.addOption("l", "list", false, "Show inventory of current backup");
    options.addOption("c", "spot-check", true, "Spot-check N backed-up files");
    options.addOption(Option.builder().longOpt("gc").desc("Delete old/unused backups").build());
    options.addOption(Option.builder().longOpt("dump-index").desc("Dump the raw index (for debugging or recovery)").build());
    options.addOption("t", "test", false, "Do a quick check of AWS actions (costs money, may leave behind some garbage)");

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
    final boolean dumpIndex = cli.hasOption("dump-index");
    final boolean local = cli.hasOption("local");
    final int numToCheck = cli.hasOption('c') ? Integer.parseInt(cli.getOptionValue('c')) : 0;
    final boolean test = cli.hasOption('t');

    if (!backup && !list && numToCheck == 0 && !gc && !dumpIndex && !test) {
      System.err.println("No action specified. Did you mean to pass '-b'?");
      return;
    }

    final String password = cli.hasOption('p') ? cli.getOptionValue('p') : Util.readPassword("Password");
    String newPassword = password;
    if (cli.hasOption('P')) {
      if (cli.getOptionValue('P').isEmpty()) {
        newPassword = Util.readPassword("New password");
        if (newPassword != null && !Util.confirmPassword(newPassword)) {
          newPassword = null;
        }
      } else {
        newPassword = cli.getOptionValue('P');
      }
    }

    if (password == null || newPassword == null) {
      System.err.println("No password; refusing to proceed");
      System.exit(1);
    }

    if (!Objects.equals(password, newPassword) && !backup) {
      System.err.println("WARNING: a new password was specified without '-b'; the new password will be ignored.");
    }

    // ------------------------------------------------------------------------------
    // Set up actors and configuration

    final Config config;
    try {
      config = loadConfig(CFG_FILE);
    } catch (FileNotFoundException e) {
      System.err.println("Config file '" + CFG_FILE + "' not found");
      System.exit(1);
      return;
    }

    final BlobTransformer transform = new XZCompression();
    final IndexFormat indexFormat = new VersionedIndexFormat(
            new JsonIndexFormatV01(),
            new JsonIndexFormatV02(),
            new JsonIndexFormatV03(),
            new JsonIndexFormatV04());
    final BackerUpper backupper;
    final UnreliableWallClock clock = UnreliableWallClock.SYSTEM_CLOCK;

    if (local) {
      var registerLocation = Paths.get("/tmp/backup/register.db");
      StringRegister register = null;
      try {
        @SuppressWarnings("required.method.not.called") // TODO
        var r = new SQLiteStringRegister(registerLocation);
        register = r;
      } catch (SQLException e) {
        System.err.println("Failed to create SQLiteStringRegister at " + registerLocation);
        e.printStackTrace();
        System.exit(1);
      }
      EventuallyConsistentDirectory dir = new LocalDirectory(Paths.get("/tmp/backup/indexes"));
      ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(register, dir);
      EventuallyConsistentBlobStore blobStore = new BlobStoreOnDirectory(new LocalDirectory(Paths.get("/tmp/backup/blobs")));
      backupper = new BackerUpper(
              indexStore, indexFormat,
              blobStore, transform,
              clock);
    } else {
      var credentials = AWSTools.credentialsProvider();

      StringRegister register = new DynamoDBStringRegister(
              DynamoDbClient.builder()
                      .credentialsProvider(credentials)
                      .region(AWS_REGION)
                      .build(),
              DYNAMO_TABLE,
              DYNAMO_REGISTER);

      EventuallyConsistentDirectory dir = new S3Directory(
              S3Client.builder()
                      .credentialsProvider(credentials)
                      .region(AWS_REGION)
                      .build(),
              S3_BUCKET);

      ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(register, dir);

      EventuallyConsistentBlobStore blobStore = new GlacierBlobStore(
              GlacierClient.builder()
                      .credentialsProvider(credentials)
                      .region(AWS_REGION)
                      .build(),
              GLACIER_VAULT_NAME);

      backupper = new BackerUpper(
              indexStore, indexFormat,
              blobStore, transform,
              clock);
    }

    // ------------------------------------------------------------------------------
    // Do the work

    if (test) {
      var credentials = AWSTools.credentialsProvider();
      StringRegister register = new DynamoDBStringRegister(
              DynamoDbClient.builder()
                      .credentialsProvider(credentials)
                      .region(AWS_REGION)
                      .build(),
              DYNAMO_TABLE,
              DYNAMO_REGISTER + ".tmp");
      String val = register.read();
      try {
        register.write(val, "test-value");
        register.write("test-value", "");
      } catch (PreconditionFailed exn) {
        System.err.println("Test failed due to concurrent interference: " + exn.getMessage());
        return;
      }
      System.out.println("Test OK");
    }

    if (backup) {
      System.out.println("Scanning filesystem...");
      List<SymLink> symlinks = new ArrayList<>();
      List<HardLink> hardlinks = new ArrayList<>();
      List<RegularFile> files = new ArrayList<>();
      Filesystem fs = new PhysicalFilesystem();
      FileTools.forEachFile(fs, config, symlinks::add, hardlinks::add, files::add);
      System.out.println("Planning backup...");
      BackerUpper.BackupPlan plan = backupper.planBackup(config.systemName(), password, newPassword, COST_MODEL, files, symlinks, hardlinks);
      System.out.println("Estimated costs:");
      System.out.println("  uploaded bytes:      " + Util.formatSize(plan.estimatedBytesUploaded()));
      System.out.println("  backup cost now:     " + plan.estimatedExecutionCost());
      System.out.println("  monthly maintenance: " + plan.estimatedMonthlyCost());
      if (!dryRun && confirm("Proceed?")) {
        try {
          plan.execute(fs);
        } catch (BackupIndex.MergeConflict mergeConflict) {
          System.err.println("Another concurrent backup interfered with this one!");
          System.err.println("Error message: " + mergeConflict.getMessage());
          System.err.println("Wait for the other backup to finish, and run the backup again.");
          System.exit(1);
        }
      }
    }

    if (list) {
      backupper.list(newPassword).forEach(info -> {
        System.out.println('[' + info.system().toString() + "] " + info.latestRevision() + ": " + info.path());
      });
    }

    if (gc) {
      System.out.println("Planning cleanup...");
      BackerUpper.CleanupPlan plan = backupper.planCleanup(password, Duration.ofDays(60), COST_MODEL);
      System.out.println("Estimated costs:");
      System.out.println("  deleted blobs:       " + plan.totalBlobsReclaimed() + " (" + plan.untrackedBlobsReclaimed() + " of which are not known to the index)");
      System.out.println("  reclaimed bytes:     " + Util.formatSize(plan.bytesReclaimed()));
      System.out.println("  backup cost now:     " + plan.estimatedExecutionCost());
      System.out.println("  monthly maintenance: " + plan.estimatedMonthlyCost());
      if (!dryRun && confirm("Proceed?")) {
        try {
          plan.execute();
        } catch (BackupIndex.MergeConflict mergeConflict) {
          System.err.println("Cleanup was skipped due to a concurrent backup.");
          System.err.println("Wait for the other backup to finish, and run the cleanup again.");
        }
      }
    }

    if (dumpIndex) {
      try (InputStream in = new BufferedInputStream(backupper.readRawIndex(newPassword))) {
        Util.copyStream(in, System.out);
      } catch (NoValue noValue) {
        System.out.println("No backups have ever been made; there is no index.");
      } catch (ConsistentBlob.TagExpired ignored) {
        System.out.println("A concurrent backup prevented the index from being read.");
        System.out.println("You may try again at your leisure.");
      }
    }

    if (numToCheck > 0) {
      List<Pair<Path, Sha256AndSize>> candidates = backupper.list(newPassword)
              .filter(item -> item.system().equals(config.systemName()))
              .filter(item -> item.latestRevision() instanceof BackupIndex.RegularFileRev)
              .map(item -> new Pair<>(item.path(), ((BackupIndex.RegularFileRev) item.latestRevision()).summary()))
              .collect(Collectors.toList());
      Random random = new Random();
      Collections.shuffle(candidates, random);
      int len = Math.min(numToCheck, candidates.size());
      List<Sha256AndSize> localSummaries = new ArrayList<>();
      List<Sha256AndSize> remoteSummaries = new ArrayList<>();
      try (ProgressDisplay display = new ProgressDisplay(len)) {
        for (int i = 0; i < len; ++i) {
          Path path = candidates.get(i).fst();
          ProgressDisplay.Task t = display.startTask("fetch " + path);
          try (InputStream in = Util.buffered(Files.newInputStream(path))) {
            localSummaries.add(Util.summarize(in, s -> { }));
          }
          Sha256AndSize thing = candidates.get(i).snd();
          try (InputStream in = Util.buffered(backupper.restore(newPassword, thing))) {
            remoteSummaries.add(Util.summarize(in, s -> display.reportProgress(t, s.getBytesRead(), thing.size())));
          }
          display.finishTask(t);
        }
      }

      boolean ok = true;
      for (int i = 0; i < len; ++i) {
        System.out.print(candidates.get(i).fst() + ": ");
        if (localSummaries.get(i).equals(remoteSummaries.get(i))) {
          System.out.println("OK");
        } else {
          System.out.println("got " + remoteSummaries.get(i));
          ok = false;
        }
      }

      if (!ok) {
        System.exit(1);
      }
    }

  }

  private static boolean confirm(String prompt) {
    Console cons = System.console();
    if (cons == null) {
      return false;
    }
    String input = cons.readLine("%s [y/n] ", prompt);
    return input != null && !input.isEmpty() && Character.toLowerCase(input.charAt(0)) == 'y';
  }

  private static class RawConfig {
    public @Nullable String system;
    public @Nullable List<String> rules;
  }

  private static Config loadConfig(Path target) throws IOException {

    JsonFactory f = new JsonFactory();
    f.enable(JsonParser.Feature.ALLOW_COMMENTS);
    ObjectMapper mapper = new ObjectMapper(f);

    RawConfig r;
    try (InputStream in = new FileInputStream(target.toString())) {
      r = mapper.readValue(in, RawConfig.class);
    }

    if (r.system == null) {
      throw new IllegalArgumentException("Config at " + target + " is missing \"system\"");
    }

    SystemId systemId = new SystemId(r.system);
    Pattern p = Pattern.compile("^(.) (.*)$");
    List<Rule> rules = new ArrayList<>();
    for (String rule : r.rules != null ? r.rules : Collections.<String>emptyList()) {
      Matcher m = p.matcher(rule);
      if (m.find()) {
        @SuppressWarnings("dereference.of.nullable") // group(1) always present
        char c = m.group(1).charAt(0);

        @SuppressWarnings("assignment") // group(2) always present
        @NonNull String ruleText = m.group(2);

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

    return new Config(systemId, rules);
  }

}
