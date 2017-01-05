package cal.bkup.impls;

import cal.bkup.AWSTools;
import cal.bkup.Util;
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
import com.amazonaws.services.glacier.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.glacier.model.InitiateMultipartUploadResult;
import com.amazonaws.services.glacier.model.UploadMultipartPartRequest;
import com.amazonaws.services.glacier.model.UploadMultipartPartResult;
import com.amazonaws.util.BinaryUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

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

  @Override
  public Id name() {
    return id;
  }

  @Override
  public Op<Id> backup(Resource r) throws IOException {
    int chunkSize = 4 * 1024 * 1024; // 4mb
    long nbytes = r.sizeEstimateInBytes();

    return new Op<Id>() {
      @Override
      public Price cost() {
        return () -> PENNIES_PER_UPLOAD_REQ.multiply(new BigDecimal(nbytes / chunkSize).add(BigDecimal.ONE));
      }

      @Override
      public Price monthlyMaintenanceCost() {
        return () -> PENNIES_PER_GB_MONTH.multiply(new BigDecimal(nbytes).add(EXTRA_BYTES_PER_ARCHIVE)).divide(ONE_GB);
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
        InputStream in = r.open();
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

        String checksum = TreeHashGenerator.calculateTreeHash(binaryChecksums);
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest()
            .withVaultName(vaultName)
            .withUploadId(id)
            .withChecksum(checksum)
            .withArchiveSize(Long.toString(total));

        CompleteMultipartUploadResult compResult = client.completeMultipartUpload(compRequest);
        return new Id(compResult.getLocation());
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
  public void close() {
  }
}
