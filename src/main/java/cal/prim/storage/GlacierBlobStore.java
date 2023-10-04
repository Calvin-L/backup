package cal.prim.storage;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.ActionCode;
import software.amazon.awssdk.services.glacier.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.glacier.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.glacier.model.CreateVaultRequest;
import software.amazon.awssdk.services.glacier.model.DeleteArchiveRequest;
import software.amazon.awssdk.services.glacier.model.DescribeJobRequest;
import software.amazon.awssdk.services.glacier.model.DescribeJobResponse;
import software.amazon.awssdk.services.glacier.model.GetJobOutputRequest;
import software.amazon.awssdk.services.glacier.model.GlacierJobDescription;
import software.amazon.awssdk.services.glacier.model.InitiateJobRequest;
import software.amazon.awssdk.services.glacier.model.InitiateJobResponse;
import software.amazon.awssdk.services.glacier.model.InitiateMultipartUploadRequest;
import software.amazon.awssdk.services.glacier.model.InitiateMultipartUploadResponse;
import software.amazon.awssdk.services.glacier.model.JobParameters;
import software.amazon.awssdk.services.glacier.model.ListJobsRequest;
import software.amazon.awssdk.services.glacier.model.ListJobsResponse;
import software.amazon.awssdk.services.glacier.model.StatusCode;
import software.amazon.awssdk.services.glacier.model.UploadArchiveRequest;
import software.amazon.awssdk.services.glacier.model.UploadArchiveResponse;
import software.amazon.awssdk.services.glacier.model.UploadMultipartPartRequest;
import software.amazon.awssdk.utils.BinaryUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class GlacierBlobStore implements EventuallyConsistentBlobStore {

  private static void createVaultIfMissing(GlacierClient client, String vaultName) {
    CreateVaultRequest req = CreateVaultRequest.builder()
            .vaultName(vaultName)
            .build();
    client.createVault(req);
  }

  /**
   * The size of each part to upload in a multipart upload.
   * Files whose sizes are smaller than this will be uploaded in one request.
   */
  private static final int UPLOAD_CHUNK_SIZE = AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK;

  private final GlacierClient client;
  private final String vaultName;

  public GlacierBlobStore(GlacierClient client, String vaultName) {
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
    } catch (AwsServiceException e) {
      throw new IOException(e);
    }
  }

  /**
   * Do a multipart upload with a big data stream.
   * @param buffer a fully-filled array of bytes (the first chunk of data)
   * @param data a stream containing the rest of the data (NOTE: this procedure modifies the
   *             array, and its size is the upload part size)
   * @return the identifier of the uploaded object
   * @throws AwsServiceException on error
   * @throws IOException on error
   */
  private PutResult doMultipartUpload(byte[] buffer, InputStream data) throws IOException {
    List<byte[]> binaryChecksums = new ArrayList<>();

    InitiateMultipartUploadResponse init = client.initiateMultipartUpload(
            InitiateMultipartUploadRequest.builder()
                    .vaultName(vaultName)
                    .partSize(Integer.toString(UPLOAD_CHUNK_SIZE))
                    .build());
    String id = init.uploadId();

    long total = 0;
    int n = buffer.length;
    do {
      String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(buffer, 0, n));
      binaryChecksums.add(BinaryUtils.fromHex(checksum));
      UploadMultipartPartRequest partRequest = UploadMultipartPartRequest.builder()
              .vaultName(vaultName)
              .uploadId(id)
              .checksum(checksum)
              .range(String.format("bytes %s-%s/*", total, total + n - 1))
              .build();

      client.uploadMultipartPart(partRequest, RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, n)));
      total += n;
      n = Util.readChunk(data, buffer);
    } while (n > 0);

    String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
    CompleteMultipartUploadRequest compRequest = CompleteMultipartUploadRequest.builder()
            .vaultName(vaultName)
            .uploadId(id)
            .checksum(checksum)
            .archiveSize(Long.toString(total))
            .build();

    CompleteMultipartUploadResponse compResult = client.completeMultipartUpload(compRequest);
    String resultId = compResult.archiveId();
    final long grandTotal = total;
    return new PutResult(resultId, grandTotal);
  }

  private PutResult uploadBytes(byte[] bytes, int n) throws IOException {
    String checksum = TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(bytes, 0, n));
    UploadArchiveRequest req = UploadArchiveRequest.builder()
            .vaultName(vaultName)
            .checksum(checksum)
            .contentLength((long)n)
            .build();
    UploadArchiveResponse res = client.uploadArchive(req, RequestBody.fromByteBuffer(ByteBuffer.wrap(bytes, 0, n)));
    String id = res.archiveId();
    return new PutResult(id, n);
  }

  private static final Path listLoc = Paths.get("/tmp/glacier-inventory");

  private InputStream loadInventory() throws IOException {
    if (!Files.exists(listLoc)) {
      // 1. find existing job
      System.out.println("Finding job...");
      String jobId = null;
      boolean complete = false;
      ListJobsResponse res = client.listJobs(ListJobsRequest.builder().vaultName(vaultName).build());
      for (GlacierJobDescription job : res.jobList()) {
        // TODO: proper handling for the ARN
        if (job.action().equals(ActionCode.INVENTORY_RETRIEVAL) &&
                job.vaultARN().contains(vaultName) &&
                !job.statusCode().equals(StatusCode.FAILED)) {
          jobId = job.jobId();
          complete = job.completed();
          System.out.println("Found job " + jobId + " [complete=" + complete + ']');
          break;
        }
      }

      // 2. if missing, initiate job
      if (jobId == null) {
        InitiateJobResponse initJobRes = client.initiateJob(InitiateJobRequest.builder()
                .vaultName(vaultName)
                .jobParameters(JobParameters.builder()
                        .type("inventory-retrieval")
                        .build())
                .build());
        jobId = initJobRes.jobId();
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

        DescribeJobResponse jobInfo = client.describeJob(DescribeJobRequest.builder()
                .vaultName(vaultName)
                .jobId(jobId)
                .build());

        System.out.println("status=" + jobInfo.statusCode());
        if (jobInfo.statusCode().equals(StatusCode.FAILED)) {
          throw new IOException("job " + jobId + " failed");
        }

        complete = jobInfo.completed();
      }

      // 4. download job result
      try (InputStream output = client.getJobOutput(GetJobOutputRequest.builder()
              .vaultName(vaultName)
              .jobId(jobId)
              .build());
           OutputStream dest = new FileOutputStream(listLoc.toString())) {
        Util.copyStream(output, dest);
      }
    }

    return new FileInputStream(listLoc.toString());
  }

  @Override
  public Stream<String> list() throws IOException {
    List<String> res = new ArrayList<>();
    try (InputStream in = loadInventory();
         JsonParser parser = new JsonFactory().createParser(in)) {
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
      client.deleteArchive(DeleteArchiveRequest.builder()
              .vaultName(vaultName)
              .archiveId(name)
              .build());
    } catch (AwsServiceException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(String name) throws IOException {
    InitiateJobResponse initJobRes = client.initiateJob(InitiateJobRequest.builder()
            .vaultName(vaultName)
            .jobParameters(JobParameters.builder()
                    .type("archive-retrieval")
                    .tier("Expedited")
                    .archiveId(name)
                    .build())
            .build());
    String jobId = initJobRes.jobId();
    System.out.println("Created new job " + jobId);

    while (true) {
      DescribeJobResponse jobInfo = client.describeJob(DescribeJobRequest.builder()
              .vaultName(vaultName)
              .jobId(jobId)
              .build());

      if (jobInfo.statusCode().equals(StatusCode.FAILED)) {
        throw new IOException("job " + jobId + " failed");
      }

      if (jobInfo.completed()) {
        System.out.println("Archive " + name + " is available");
        return client.getJobOutput(GetJobOutputRequest.builder()
                .vaultName(vaultName)
                .jobId(jobId)
                .build());
      }
      try {
        Thread.sleep(1000L * 60L * 5L); // sleep 5 minutes
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

}
