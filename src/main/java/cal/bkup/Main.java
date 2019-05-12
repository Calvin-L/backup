package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.BackupIndex;
import cal.bkup.impls.JsonIndexFormat;
import cal.bkup.impls.ProgressDisplay;
import cal.bkup.types.Config;
import cal.bkup.types.SystemId;
import cal.prim.NoValue;
import cal.prim.fs.HardLink;
import cal.bkup.types.IndexFormat;
import cal.prim.fs.RegularFile;
import cal.bkup.types.Rule;
import cal.bkup.types.Sha256AndSize;
import cal.bkup.types.StorageCostModel;
import cal.prim.fs.SymLink;
import cal.prim.BlobStoreOnDirectory;
import cal.prim.ConsistentBlob;
import cal.prim.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.DynamoDBStringRegister;
import cal.prim.EventuallyConsistentBlobStore;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.GlacierBlobStore;
import cal.prim.LocalDirectory;
import cal.prim.Pair;
import cal.prim.Price;
import cal.prim.S3Directory;
import cal.prim.SQLiteStringRegister;
import cal.prim.StringRegister;
import cal.prim.transforms.BlobTransformer;
import cal.prim.transforms.XZCompression;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.glacier.AmazonGlacierClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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

  private static final String AWS_REGION = "us-east-2";
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

    if (!backup && !list && numToCheck == 0 && !gc && !dumpIndex) {
      System.err.println("No action specified. Did you mean to pass '-b'?");
      return;
    }

    final String password = cli.hasOption('p') ? cli.getOptionValue('p') : Util.readPassword("Password");
    final String newPassword = cli.hasOption('P')
            ? (cli.getOptionValue('P').isEmpty() ? Util.readPassword("New password") : cli.getOptionValue('P'))
            : password;

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
    final IndexFormat indexFormat = new JsonIndexFormat();
    final BackerUpper backupper;

    if (local) {
      var registerLocation = Paths.get("/tmp/backup/register.db");
      StringRegister register = null;
      try {
        register = new SQLiteStringRegister(registerLocation);
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
              blobStore, transform);
    } else {
      StringRegister register = new DynamoDBStringRegister(
              new DynamoDB(AmazonDynamoDBClientBuilder
                      .standard()
                      .withRegion(AWS_REGION)
                      .withCredentials(AWSTools.credentialsProvider())
                      .build()),
              DYNAMO_TABLE,
              DYNAMO_REGISTER);

      EventuallyConsistentDirectory dir = new S3Directory(
              AmazonS3ClientBuilder
                      .standard()
                      .withCredentials(AWSTools.credentialsProvider())
                      .withRegion(AWS_REGION)
                      .build(),
              S3_BUCKET);

      ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(register, dir);

      EventuallyConsistentBlobStore blobStore = new GlacierBlobStore(
              AmazonGlacierClientBuilder
                      .standard()
                      .withCredentials(AWSTools.credentialsProvider())
                      .withRegion(AWS_REGION)
                      .build(),
              GLACIER_VAULT_NAME);

      backupper = new BackerUpper(
              indexStore, indexFormat,
              blobStore, transform);
    }

    // ------------------------------------------------------------------------------
    // Do the work

    if (backup) {
      System.out.println("Scanning filesystem...");
      List<SymLink> symlinks = new ArrayList<>();
      List<HardLink> hardlinks = new ArrayList<>();
      List<RegularFile> files = new ArrayList<>();
      FileTools.forEachFile(config, symlinks::add, hardlinks::add, files::add);
      System.out.println("Planning backup...");
      BackerUpper.BackupPlan plan = backupper.planBackup(config.getSystemName(), password, newPassword, COST_MODEL, files, symlinks, hardlinks);
      System.out.println("Estimated costs:");
      System.out.println("  uploaded bytes:      " + Util.formatSize(plan.estimatedBytesUploaded()));
      System.out.println("  backup cost now:     " + plan.estimatedExecutionCost());
      System.out.println("  monthly maintenance: " + plan.estimatedMonthlyCost());
      if (!dryRun && confirm("Proceed?")) {
        try {
          plan.execute();
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
        System.out.println('[' + info.system().toString() + "] " + info.path());
      });
    }

    if (gc) {
      backupper.cleanup();
    }

    if (dumpIndex) {
      try (InputStream in = new BufferedInputStream(backupper.readRawIndex(newPassword))) {
        Util.copyStream(in, System.out);
      } catch (NoValue noValue) {
        System.out.println("No backups have ever been made; there is no index.");
      }
    }

    if (numToCheck > 0) {
      List<Pair<Path, Sha256AndSize>> candidates = backupper.list(newPassword)
              .filter(item -> item.system().equals(config.getSystemName()) && item.latestRevision().type == BackupIndex.FileType.REGULAR_FILE)
              .map(item -> new Pair<>(item.path(), item.latestRevision().summary))
              .collect(Collectors.toList());
      Random random = new Random();
      Collections.shuffle(candidates, random);
      int len = Math.min(numToCheck, candidates.size());
      List<Sha256AndSize> localSummaries = new ArrayList<>();
      List<Sha256AndSize> remoteSummaries = new ArrayList<>();
      try (ProgressDisplay display = new ProgressDisplay(len)) {
        for (int i = 0; i < len; ++i) {
          Path path = candidates.get(i).getFst();
          ProgressDisplay.Task t = display.startTask("fetch " + path);
          try (InputStream in = Util.buffered(Files.newInputStream(path))) {
            localSummaries.add(Util.summarize(in, s -> { }));
          }
          Sha256AndSize thing = candidates.get(i).getSnd();
          try (InputStream in = Util.buffered(backupper.restore(newPassword, thing))) {
            remoteSummaries.add(Util.summarize(in, s -> display.reportProgress(t, s.getBytesRead(), thing.getSize())));
          }
          display.finishTask(t);
        }
      }

      boolean ok = true;
      for (int i = 0; i < len; ++i) {
        System.out.print(candidates.get(i).getFst() + ": ");
        if (localSummaries.get(i).equals(remoteSummaries.get(i))) {
          System.out.println("OK");
        } else {
          System.out.println("got " + remoteSummaries.get(i));
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
    return input.length() > 0 && Character.toLowerCase(input.charAt(0)) == 'y';
  }

  private static class RawConfig {
    public String system;
    public List<String> rules;
  }

  private static Config loadConfig(Path target) throws IOException {

    JsonFactory f = new JsonFactory();
    f.enable(JsonParser.Feature.ALLOW_COMMENTS);
    ObjectMapper mapper = new ObjectMapper(f);

    RawConfig r;
    try (InputStream in = new FileInputStream(target.toString())) {
      r = mapper.readValue(in, RawConfig.class);
    }

    SystemId systemId = new SystemId(r.system);
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

    return new Config(systemId, rules);
  }

}
