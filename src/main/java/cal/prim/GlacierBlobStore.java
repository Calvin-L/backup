package cal.prim;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.glacier.AmazonGlacier;
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
import com.amazonaws.util.BinaryUtils;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class GlacierBlobStore implements EventuallyConsistentBlobStore {

  private static void createVaultIfMissing(AmazonGlacier client, String vaultName) {
    CreateVaultRequest req = new CreateVaultRequest()
            .withVaultName(vaultName);
    client.createVault(req);
  }

  /**
   * The size of each part to upload in a multipart upload.
   * Files whose sizes are smaller than this will be uploaded in one request.
   */
  private static final int UPLOAD_CHUNK_SIZE = AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK;

  private final AmazonGlacier client;
  private final String vaultName;

  public GlacierBlobStore(AmazonGlacier client, String vaultName) {
    createVaultIfMissing(client, vaultName);
    this.client = client;
    this.vaultName = vaultName;
  }

  @Override
  public PutResult put(InputStream data) throws IOException {
    try {
      byte[] buffer = new byte[AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK];
      int n = Util.readChunk(data, buffer);
      if (n < buffer.length) {
        return uploadBytes(buffer, n);
      } else {
        return doMultipartUpload(buffer, data);
      }
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  /**
   * Do a multipart upload with a big data stream.
   * @param buffer a fully-filled array of bytes (the first chunk of data)
   * @param data a stream containing the rest of the data (NOTE: this procedure modifies the
   *             array, and its size is the upload part size)
   * @return the identifier of the uploaded object
   * @throws SdkClientException on error
   * @throws IOException on error
   */
  private PutResult doMultipartUpload(byte[] buffer, InputStream data) throws IOException {
    List<byte[]> binaryChecksums = new ArrayList<>();

    InitiateMultipartUploadResult init = client.initiateMultipartUpload(
            new InitiateMultipartUploadRequest()
                    .withVaultName(vaultName)
                    .withPartSize(Integer.toString(UPLOAD_CHUNK_SIZE)));
    String id = init.getUploadId();

    long total = 0;
    int n = buffer.length;
    do {
      String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(buffer, 0, n));
      binaryChecksums.add(BinaryUtils.fromHex(checksum));
      UploadMultipartPartRequest partRequest = new UploadMultipartPartRequest()
              .withVaultName(vaultName)
              .withUploadId(id)
              .withBody(new ByteArrayInputStream(buffer, 0, n))
              .withChecksum(checksum)
              .withRange(String.format("bytes %s-%s/*", total, total + n - 1));

      client.uploadMultipartPart(partRequest);
      total += n;
      n = Util.readChunk(data, buffer);
    } while (n > 0);

    String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
    CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
            .withVaultName(vaultName)
            .withUploadId(id)
            .withChecksum(checksum)
            .withArchiveSize(Long.toString(total));

    CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
    String resultId = compResult.getArchiveId();
    final long grandTotal = total;
    return new PutResult(resultId, grandTotal);
  }

  private PutResult uploadBytes(byte[] bytes, int n) {
    String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytes, 0, n));
    UploadArchiveRequest req = new UploadArchiveRequest()
            .withVaultName(vaultName)
            .withChecksum(checksum)
            .withContentLength((long)n)
            .withBody(new ByteArrayInputStream(bytes, 0, n));
    UploadArchiveResult res = client.uploadArchive(req);
    String id = res.getArchiveId();
    return new PutResult(id, n);
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
  public Stream<String> list() throws IOException {
    List<String> res = new ArrayList<>();
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
              res.add(archiveId);
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
  public void delete(String name) throws IOException {
    try {
      client.deleteArchive(new DeleteArchiveRequest()
              .withVaultName(vaultName)
              .withArchiveId(name));
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(String name) throws IOException {
    InitiateJobResult initJobRes = client.initiateJob(new InitiateJobRequest()
            .withVaultName(vaultName)
            .withJobParameters(new JobParameters()
                    .withType("archive-retrieval")
                    .withTier("Expedited")
                    .withArchiveId(name)));
    String jobId = initJobRes.getJobId();
    System.out.println("Created new job " + jobId);

    while (true) {
      DescribeJobResult jobInfo = client.describeJob(new DescribeJobRequest()
              .withVaultName(vaultName)
              .withJobId(jobId));

      if (jobInfo.getStatusCode().equals(StatusCode.Failed.toString())) {
        throw new IOException("job " + jobId + " failed");
      }

      if (jobInfo.isCompleted()) {
        System.out.println("Archive " + name + " is available");
        GetJobOutputResult outputResult = client.getJobOutput(new GetJobOutputRequest()
                .withVaultName(vaultName)
                .withJobId(jobId));
        return outputResult.getBody();
      }
      try {
        Thread.sleep(1000L * 60L * 5L); // sleep 5 minutes
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

}
