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
import com.amazonaws.util.BinaryUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
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
  private static final BigDecimal PENNIES_PER_UPLOAD_REQ = new BigDecimal(0.05).multiply(new BigDecimal(100)).divide(new BigDecimal(1000));
  private static final BigDecimal PENNIES_PER_GB_MONTH = new BigDecimal(0.004).multiply(new BigDecimal(100));
  private static final BigDecimal EXTRA_BYTES_PER_ARCHIVE = new BigDecimal(32 * 1024); // 32 Kb
  private static final BigDecimal ONE_GB = new BigDecimal(1024L * 1024L * 1024L);

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
    BigDecimal value = PENNIES_PER_GB_MONTH.multiply(new BigDecimal(archiveSizeInBytes).add(EXTRA_BYTES_PER_ARCHIVE)).divide(ONE_GB);
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
        return () -> PENNIES_PER_UPLOAD_REQ.multiply(new BigDecimal(nbytes / chunkSize).add(BigDecimal.ONE));
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

  @Override
  public Stream<BackedUpResourceInfo> list() throws IOException {
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
      System.out.println("Waiting for completion...");
      try {
        Thread.sleep(1000L * 60L * 5L); // sleep 5 minutes
      } catch (InterruptedException e) {
        throw new IOException(e);
      }

      DescribeJobResult jobInfo = client.describeJob(new DescribeJobRequest()
          .withVaultName(vaultName)
          .withJobId(jobId));

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
         OutputStream dest = new FileOutputStream("/tmp/glacier-inventory")) {
      Util.copyStream(output, dest);
    }

    throw new UnsupportedOperationException();
  }

  @Override
  public Op<Void> delete(BackedUpResourceInfo obj) {
    return new Op<Void>() {
      @Override
      public Price cost() {
        return Price.ZERO;
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
    };
  }

  @Override
  public void close() {
  }
}
