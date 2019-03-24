package cal.bkup;

import cal.bkup.impls.BackerUpper;
import cal.bkup.impls.JsonIndexFormat;
import cal.bkup.types.StorageCostModel;
import cal.bkup.types.Config;
import cal.bkup.types.HardLink;
import cal.bkup.types.Id;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.RegularFile;
import cal.bkup.types.Rule;
import cal.bkup.types.SymLink;
import cal.prim.BlobStoreOnDirectory;
import cal.prim.ConsistentBlob;
import cal.prim.ConsistentBlobOnEventuallyConsistentDirectory;
import cal.prim.DynamoDBStringRegister;
import cal.prim.EventuallyConsistentBlobStore;
import cal.prim.EventuallyConsistentDirectory;
import cal.prim.GlacierBlobStore;
import cal.prim.LocalDirectory;
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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.math3.fraction.BigFraction;

import java.io.Console;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

  private static final String AWS_REGION = "us-east-2";
  private static final String GLACIER_VAULT_NAME = "mybackups";
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
              () -> PENNIES_PER_UPLOAD_REQ :
              () -> PENNIES_PER_UPLOAD_REQ.multiply(new BigFraction(numBytes / AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK).add(BigFraction.TWO));
    }

    @Override
    public Price monthlyStorageCostForBlob(long numBytes) {
      BigFraction value = PENNIES_PER_GB_MONTH.multiply(new BigFraction(numBytes).add(EXTRA_BYTES_PER_ARCHIVE)).divide(ONE_GB);
      return () -> value;
    }
  };

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

    if (!backup && !list && numToCheck == 0 && !gc) {
      System.err.println("No action specified. Did you mean to pass '-b'?");
      return;
    }

    final String password = cli.hasOption('p') ? cli.getOptionValue('p') : Util.readPassword();

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
      StringRegister register = new SQLiteStringRegister(Paths.get("/tmp/backup/register.db"));
      EventuallyConsistentDirectory dir = new LocalDirectory(Paths.get("/tmp/backup/indexes"));
      ConsistentBlob indexStore = new ConsistentBlobOnEventuallyConsistentDirectory(register, dir);
      EventuallyConsistentBlobStore blobStore = new BlobStoreOnDirectory(new LocalDirectory(Paths.get("/tmp/backup/blobs")));
      backupper = new BackerUpper(
              indexStore, indexFormat,
              blobStore, transform);
    } else {
      if (true) throw new UnsupportedOperationException();

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
      BackerUpper.BackupPlan plan = backupper.planBackup(config.systemName(), password, password, COST_MODEL, files, symlinks, hardlinks);
      System.out.println("Estimated costs:");
      System.out.println("  uploaded bytes:      " + Util.formatSize(plan.estimatedBytesUploaded()));
      System.out.println("  backup cost now:     " + Util.formatPrice(plan.estimatedExecutionCost()));
      System.out.println("  monthly maintenance: " + Util.formatPrice(plan.estimatedMonthlyCost()));
      if (!dryRun && confirm("Proceed?")) {
        plan.execute();
      }
    }

    if (list) {
      backupper.list(password).forEach(info -> {
        System.out.println('[' + info.system().toString() + "] " + info.path());
      });
    }

    if (gc) {
      backupper.cleanup();
    }

    if (numToCheck > 0) {
      throw new UnsupportedOperationException();
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

}
