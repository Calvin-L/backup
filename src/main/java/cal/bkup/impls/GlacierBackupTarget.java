package cal.bkup.impls;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import cal.bkup.types.BackedUpResourceInfo;
import cal.bkup.types.BackupReport;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Op;
import cal.bkup.types.Pair;
import cal.bkup.types.Price;
import cal.bkup.types.ResourceInfo;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class GlacierBackupTarget implements BackupTarget {

  private static void createVaultIfMissing(AmazonGlacier client, String vaultName) {
    CreateVaultRequest req = new CreateVaultRequest()
        .withVaultName(vaultName);
    client.createVault(req);
  }

  private static final int UPLOAD_CHUNK_SIZE = AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK;

  /**
   * Files whose sizes are smaller than this will be uploaded in one request
   */
  private static final int ONESHOT_UPLOAD_CUTOFF = UPLOAD_CHUNK_SIZE;

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
  public Price estimatedCostOfDataTransfer(long resourceSizeInBytes) {
    return resourceSizeInBytes <= ONESHOT_UPLOAD_CUTOFF ?
            () -> PENNIES_PER_UPLOAD_REQ :
            () -> PENNIES_PER_UPLOAD_REQ.multiply(new BigFraction(resourceSizeInBytes / UPLOAD_CHUNK_SIZE).add(BigFraction.ONE));
  }

  @Override
  public Price estimatedMonthlyMaintenanceCost(long resourceSizeInBytes) {
    return maintenanceCostForArchive(resourceSizeInBytes);
  }

  @Override
  public BackupReport backup(InputStream data, long estimatedByteCount) throws IOException {
    if (estimatedByteCount <= ONESHOT_UPLOAD_CUTOFF) {
      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      Util.copyStream(data, buf);
      byte[] bytes = buf.toByteArray();

      String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytes));
      UploadArchiveRequest req = new UploadArchiveRequest()
              .withVaultName(vaultName)
              .withChecksum(checksum)
              .withContentLength((long)bytes.length)
              .withBody(new ByteArrayInputStream(bytes));
      UploadArchiveResult res = client.uploadArchive(req);
      Id id = new Id(res.getArchiveId());
      return new BackupReport() {
        @Override
        public long sizeAtTarget() {
          return bytes.length;
        }

        @Override
        public Id idAtTarget() {
          return id;
        }
      };
    }

    byte[] chunk = new byte[UPLOAD_CHUNK_SIZE];
    List<byte[]> binaryChecksums = new ArrayList<>();

    InitiateMultipartUploadResult init = client.initiateMultipartUpload(
        new InitiateMultipartUploadRequest()
            .withVaultName(vaultName)
            .withPartSize(Integer.toString(UPLOAD_CHUNK_SIZE)));
    String id = init.getUploadId();

    long total = 0;
    int n;
    while ((n = Util.readChunk(data, chunk)) > 0) {
      String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(chunk, 0, n));
      binaryChecksums.add(BinaryUtils.fromHex(checksum));
      UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
          .withVaultName(vaultName)
          .withUploadId(id)
          .withBody(new ByteArrayInputStream(chunk, 0, n))
          .withChecksum(checksum)
          .withRange(String.format("bytes %s-%s/*", total, total + n - 1));

      UploadMultipartPartResult partResult = client.uploadMultipartPart(partRequest);
      System.out.println("Part uploaded [file=???], checksum: " + partResult.getChecksum());
      total += n;
    }

    String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
        .withVaultName(vaultName)
        .withUploadId(id)
        .withChecksum(checksum)
        .withArchiveSize(Long.toString(total));

    CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
    Id backupId = new Id(compResult.getArchiveId());
    final long finalSize = total;
    return new BackupReport() {
      @Override
      public long sizeAtTarget() {
        return finalSize;
      }

      @Override
      public Id idAtTarget() {
        return backupId;
      }
    };
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
                public long storedSizeInBytes() {
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
                .multiply(new BigFraction(obj.storedSizeInBytes()).add(EXTRA_BYTES_PER_ARCHIVE))
                .multiply(new BigFraction(earlyDeletion.get(ChronoUnit.SECONDS)))
                .multiply(MONTHS_PER_SECOND)
                .divide(ONE_GB);
      }

      @Override
      public Price monthlyMaintenanceCost() {
        return () -> maintenanceCostForArchive(obj.storedSizeInBytes()).valueInCents().negate();
      }

      @Override
      public long estimatedDataTransfer() {
        return 0;
      }

      @Override
      public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
        client.deleteArchive(new DeleteArchiveRequest()
            .withVaultName(vaultName)
            .withArchiveId(obj.idAtTarget().toString()));
        return null;
      }

      @Override
      public String toString() {
        return "delete archive " + vaultName + '/' + obj.idAtTarget() + " [" + Util.formatSize(obj.storedSizeInBytes()) + ']';
      }
    };
  }

  @Override
  public Op<Void> fetch(Collection<ResourceInfo> infos, IOConsumer<Pair<ResourceInfo, InputStream>> callback) throws IOException {
    long totalBytes = infos.stream().mapToLong(ResourceInfo::sizeAtTarget).sum();
    return new Op<Void>() {
      @Override
      public Price cost() {
        BigFraction res = (PENNIES_PER_DOWNLOAD_GB_REQ.add(PENNIES_PER_DOWNLOAD_GB_XFER)).multiply(totalBytes);
        return () -> res;
      }

      @Override
      public Price monthlyMaintenanceCost() {
        return Price.ZERO;
      }

      @Override
      public long estimatedDataTransfer() {
        return totalBytes;
      }

      @Override
      public Void exec(ProgressDisplay.ProgressCallback progressCallback) throws IOException {
        Map<ResourceInfo, String> jobs = new HashMap<>();
        for (ResourceInfo i : infos) {
          InitiateJobResult initJobRes = client.initiateJob(new InitiateJobRequest()
              .withVaultName(vaultName)
              .withJobParameters(new JobParameters()
                  .withType("archive-retrieval")
                  .withArchiveId(i.idAtTarget().toString())));
          String jobId = initJobRes.getJobId();
          System.out.println("Created new job " + jobId);
          jobs.put(i, jobId);
        }

        while (!jobs.isEmpty()) {
          System.out.println("Waiting on " + jobs.size() + " jobs...");
          Iterator<Map.Entry<ResourceInfo, String>> it = jobs.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry<ResourceInfo, String> e = it.next();
            String jobId = e.getValue();

            DescribeJobResult jobInfo = client.describeJob(new DescribeJobRequest()
                .withVaultName(vaultName)
                .withJobId(jobId));

            if (jobInfo.getStatusCode().equals(StatusCode.Failed.toString())) {
              throw new IOException("job " + jobId + " failed");
            }

            if (jobInfo.isCompleted()) {
              ResourceInfo i = e.getKey();
              it.remove();
              System.out.println("Archive " + i.idAtTarget() + " is available");
              GetJobOutputResult outputResult = client.getJobOutput(new GetJobOutputRequest()
                  .withVaultName(vaultName)
                  .withJobId(jobId));
              callback.accept(new Pair<>(i, outputResult.getBody()));
            }
          }
          try {
            Thread.sleep(1000L * 60L * 5L); // sleep 5 minutes
          } catch (InterruptedException e) {
            throw new IOException(e);
          }
        }
        return null;
      }

      @Override
      public String toString() {
        return "fetch " + infos.size() + " files";
      }
    };
  }

  @Override
  public void close() {
  }
}
