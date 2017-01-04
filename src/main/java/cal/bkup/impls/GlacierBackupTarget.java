package cal.bkup.impls;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import cal.bkup.types.BackupTarget;
import cal.bkup.types.IOConsumer;
import cal.bkup.types.Id;
import cal.bkup.types.Resource;
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
import java.util.ArrayList;
import java.util.List;

public class GlacierBackupTarget implements BackupTarget {

  private static void createVaultIfMissing(AmazonGlacierClient client, String vaultName) {
    CreateVaultRequest req = new CreateVaultRequest()
        .withVaultName(vaultName);
    client.createVault(req);
  }

  private final Id id;
  private final AmazonGlacierClient client;
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
  public void backup(Resource r, IOConsumer<Id> k) throws IOException {
    int chunkSize = 4 * 1024 * 1024; // 4mb
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
    k.accept(new Id(compResult.getLocation()));
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
