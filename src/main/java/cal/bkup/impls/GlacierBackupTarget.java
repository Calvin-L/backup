package cal.bkup.impls;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Price;
import cal.bkup.types.Resource;
import com.amazonaws.services.glacier.AmazonGlacier;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.glacier.model.CompleteMultipartUploadResult;
import com.amazonaws.services.glacier.model.CreateVaultRequest;
import com.amazonaws.services.glacier.model.DeleteArchiveRequest;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.GlacierJobDescription;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.ListJobsRequest;
import com.amazonaws.services.glacier.model.ListJobsResult;
import com.amazonaws.services.glacier.model.StatusCode;
import com.amazonaws.services.glacier.model.UploadArchiveRequest;
import com.amazonaws.services.glacier.model.UploadArchiveResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartResult;
import com.amazonaws.services.kms.model.UnsupportedOperationException;
import com.amazonaws.util.BinaryUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.commons.math3.fraction.BigFraction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class GlacierBackupTarget implements BackupTarget {

  private static void createVaultIfMissing(AmazonGlacier client, String vaultName) {
    CreateVaultRequest req = new CreateVaultRequest()
        .withVaultName(vaultName);
    client.createVault(req);
  }

  // See https://aws.amazon.com/glacier/pricing/
  // See https://aws.amazon.com/glacier/faqs/
  private static final BigFraction PENNIES_PER_UPLOAD_REQ = new BigFraction(0.05).multiply(new BigFraction(100)).divide(new BigFraction(1000));
  private static final BigFraction PENNIES_PER_GB_MONTH = new BigFraction(0.004).multiply(new BigFraction(100));
  private static final BigFraction EXTRA_BYTES_PER_ARCHIVE = new BigFraction(32 * 1024); // 32 Kb
  private static final BigFraction ONE_GB = new BigFraction(1024L * 1024L * 1024L);
  private static final Duration ONE_MONTH = Duration.of(30, ChronoUnit.DAYS);
  private static final Duration MINIMUM_STORAGE_DURATION = ONE_MONTH.multipliedBy(3);
  private static final BigFraction EARLY_DELETION_FEE_PER_GB_MONTH = PENNIES_PER_GB_MONTH;
  private static final BigFraction MONTHS_PER_SECOND = BigFraction.ONE.divide(new BigFraction(ONE_MONTH.get(ChronoUnit.SECONDS)));

  private final Id id;
  private final AmazonGlacier client;
  private final String vaultName;

  public GlacierBackupTarget(String endpoint, String vaultName) {
    Util.ensure(endpoint.indexOf(':') < 0);
    Util.ensure(vaultName.indexOf(':') < 0);
    id = new Id("glacier:" + endpoint + ":" + vaultName);
    client = new AmazonGlacierClient(AWSTools.getCredentials());
    client.setEndpoint(endpoint);
    createVaultIfMissing(client, vaultName);
    this.vaultName = vaultName;
  }

  private static Price maintenanceCostForArchive(long archiveSizeInBytes) {
    BigFraction value = PENNIES_PER_GB_MONTH.multiply(new BigFraction(archiveSizeInBytes).add(EXTRA_BYTES_PER_ARCHIVE)).divide(ONE_GB);
    return () -> value;
  }

  @Override
  public Id name() {
    return id;
  }

  @Override
  public Op<Id> backup(Resource r) throws IOException {
    int chunkSize = 4 * 1024 * 1024; // 4mb
    long nbytes = r.sizeEstimateInBytes();

    if (nbytes <= chunkSize * 2) {
      return new Op<Id>() {
        @Override
        public Price cost() {
          return () -> PENNIES_PER_UPLOAD_REQ;
        }

        @Override
        public Price monthlyMaintenanceCost() {
          return maintenanceCostForArchive(nbytes);
        }

        @Override
        public long estimatedDataTransfer() {
          return nbytes;
        }

        @Override
        public Id exec() throws IOException {
          ByteArrayOutputStream buf = new ByteArrayOutputStream();
          try (InputStream in = r.open()) {
            Util.copyStream(in, buf);
          }
          byte[] bytes = buf.toByteArray();

          String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytes));
          UploadArchiveRequest req = new UploadArchiveRequest()
              .withVaultName(vaultName)
              .withChecksum(checksum)
              .withContentLength((long)bytes.length)
              .withBody(new ByteArrayInputStream(bytes));
          UploadArchiveResult res = client.uploadArchive(req);
          return new Id(res.getArchiveId());
        }

        @Override
        public String toString() {
          return r.path().toString();
        }
      };
    }

    return new Op<Id>() {
      @Override
      public Price cost() {
        return () -> PENNIES_PER_UPLOAD_REQ.multiply(new BigFraction(nbytes / chunkSize).add(BigFraction.TWO));
      }

      @Override
      public Price monthlyMaintenanceCost() {
        return maintenanceCostForArchive(nbytes);
      }

      @Override
      public long estimatedDataTransfer() {
        return nbytes;
      }

      @Override
      public Id exec() throws IOException {
        byte[] chunk = new byte[chunkSize];
        List<byte[]> binaryChecksums = new ArrayList<>();

        InitiateMultipartUploadResult init = client.initiateMultipartUpload(
            new InitiateMultipartUploadRequest()
                .withVaultName(vaultName)
                .withPartSize(Integer.toString(chunkSize)));
        String id = init.getUploadId();

        long total = 0;
        try (InputStream in = r.open()) {
          int n;
          while ((n = readChunk(in, chunk)) > 0) {
            String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(chunk, 0, n));
            binaryChecksums.add(BinaryUtils.fromHex(checksum));
            UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
                .withVaultName(vaultName)
                .withUploadId(id)
                .withBody(new ByteArrayInputStream(chunk, 0, n))
                .withChecksum(checksum)
                .withRange(String.format("bytes %s-%s/*", total, total + n - 1));

            UploadMultipartPartResult partResult = client.uploadMultipartPart(partRequest);
            System.out.println("Part uploaded [" + r.path().getFileName() + "], checksum: " + partResult.getChecksum());
            total += n;
          }
        }

        String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
            .withVaultName(vaultName)
            .withUploadId(id)
            .withChecksum(checksum)
            .withArchiveSize(Long.toString(total));

        CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
        return new Id(compResult.getArchiveId());
      }

      @Override
      public String toString() {
        return r.path().toString();
      }
    };
  }

  private static int readChunk(InputStream in, byte[] chunk) throws IOException {
    int soFar = 0;
    int n;
    while (soFar < chunk.length && (n = in.read(chunk, soFar, chunk.length - soFar)) >= 0) {
      soFar += n;
    }
    return soFar;
  }

  private static final Path listLoc = Paths.get("/tmp/glacier-inventory");

  private InputStream loadInventory() throws IOException {
    if (!Files.exists(listLoc)) {
      // 1. find existing job
      System.out.println("Finding job...");
      String jobId = null;
      boolean complete = false;
      ListJobsResult res = client.listJobs(new ListJobsRequest().withVaultName(vaultName));
      for (GlacierJobDescription job : res.getJobList()) {
        // TODO: proper handling for the ARN
        if (job.getAction().equals("InventoryRetrieval") &&
            job.getVaultARN().contains(vaultName) &&
            !job.getStatusCode().equals(StatusCode.Failed.toString())) {
          jobId = job.getJobId();
          complete = job.getCompleted();
          System.out.println("Found job " + jobId + " [complete=" + complete + ']');
          break;
        }
      }

      // 2. if missing, initiate job
      if (jobId == null) {
        InitiateJobResult initJobRes = client.initiateJob(new InitiateJobRequest()
            .withVaultName(vaultName)
            .withJobParameters(new JobParameters()
                .withType("inventory-retrieval")));
        jobId = initJobRes.getJobId();
        System.out.println("Created new job " + jobId);
      }

      // 3. wait for job to complete
      while (!complete) {
        System.out.print("Waiting for inventory... ");
        System.out.flush();
        try {
          Thread.sleep(1000L * 60L * 5L); // sleep 5 minutes
        } catch (InterruptedException e) {
          throw new IOException(e);
        }

        DescribeJobResult jobInfo = client.describeJob(new DescribeJobRequest()
            .withVaultName(vaultName)
            .withJobId(jobId));

        System.out.println("status=" + jobInfo.getStatusCode());
        if (jobInfo.getStatusCode().equals(StatusCode.Failed.toString())) {
          throw new IOException("job " + jobId + " failed");
        }

        complete = jobInfo.isCompleted();
      }

      // 4. download job result
      GetJobOutputResult outputResult = client.getJobOutput(new GetJobOutputRequest()
          .withVaultName(vaultName)
          .withJobId(jobId));

      try (InputStream output = outputResult.getBody();
           OutputStream dest = new FileOutputStream(listLoc.toString())) {
        Util.copyStream(output, dest);
      }
    }

    return new FileInputStream(listLoc.toString());
  }

  @Override
  public Stream<BackedUpResourceInfo> list() throws IOException {
    List<BackedUpResourceInfo> res = new ArrayList<>();
    try (InputStream in = loadInventory()) {
      JsonParser parser = new JsonFactory().createParser(in);
      JsonToken tok;
      String currentField = null;
      String archiveId = null;
      String creationDate = null;
      Long size = null;
      while ((tok = parser.nextToken()) != null) {
        switch (tok) {
          case NOT_AVAILABLE:
            continue;
          case START_OBJECT:
            currentField = null;
            archiveId = null;
            size = null;
            creationDate = null;
            break;
          case END_OBJECT:
            if (archiveId != null && creationDate != null && size != null) {
              Id id = new Id(archiveId);
              long sz = size;
              Instant time = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(creationDate));
              res.add(new BackedUpResourceInfo() {
                @Override
                public Id idAtTarget() {
                  return id;
                }

                @Override
                public long sizeInBytes() {
                  return sz;
                }

                @Override
                public Instant backupTime() {
                  return time;
                }
              });
            }
            break;
          case FIELD_NAME:
            currentField = parser.getCurrentName();
            break;
          case VALUE_EMBEDDED_OBJECT:
            throw new UnsupportedOperationException(tok.toString());
          case VALUE_STRING:
            if ("ArchiveId".equals(currentField)) {
              archiveId = parser.getText();
            } else if ("CreationDate".equals(currentField)) {
              creationDate = parser.getText();
            }
            break;
          case VALUE_NUMBER_INT:
            if ("Size".equals(currentField)) {
              size = parser.getLongValue();
            }
            break;
          case START_ARRAY:
          case END_ARRAY:
          case VALUE_NUMBER_FLOAT:
          case VALUE_TRUE:
          case VALUE_FALSE:
          case VALUE_NULL:
            break;
        }
      }
    }
    return res.stream();
  }

  @Override
  public Op<Void> delete(BackedUpResourceInfo obj) {
    Instant now = Instant.now();
    return new Op<Void>() {
      @Override
      public Price cost() {
        Duration lifetime = Duration.between(obj.backupTime(), now);
        Duration earlyDeletion = MINIMUM_STORAGE_DURATION.minus(lifetime);
        return earlyDeletion.isNegative() ?
            Price.ZERO :
            () -> EARLY_DELETION_FEE_PER_GB_MONTH
                .multiply(new BigFraction(obj.sizeInBytes()).add(EXTRA_BYTES_PER_ARCHIVE))
                .multiply(new BigFraction(earlyDeletion.get(ChronoUnit.SECONDS)))
                .multiply(MONTHS_PER_SECOND)
                .divide(ONE_GB);
      }

      @Override
      public Price monthlyMaintenanceCost() {
        return () -> maintenanceCostForArchive(obj.sizeInBytes()).valueInCents().negate();
      }

      @Override
      public long estimatedDataTransfer() {
        return 0;
      }

      @Override
      public Void exec() throws IOException {
        client.deleteArchive(new DeleteArchiveRequest()
            .withVaultName(vaultName)
            .withArchiveId(obj.idAtTarget().toString()));
        return null;
      }

      @Override
      public String toString() {
        return "delete archive " + vaultName + '/' + obj.idAtTarget() + " [" + Util.formatSize(obj.sizeInBytes()) + ']';
      }
    };
  }

  @Override
  public void close() {
  }
}
