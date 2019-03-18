package cal.prim;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3Directory implements SimpleDirectory, EventuallyConsistentDirectory {

  private final String bucket;
  private final AmazonS3 s3client;

  public S3Directory(AmazonS3 s3client, String bucketName) {
    this.bucket = bucketName;
    this.s3client = s3client;
    if (!s3client.doesBucketExistV2(bucketName)) {
      s3client.createBucket(bucketName);
    }
//    s3client.setBucketLifecycleConfiguration(
//        new SetBucketLifecycleConfigurationRequest(bucketName,
//            new BucketLifecycleConfiguration().withRules(Collections.singletonList(
//                new BucketLifecycleConfiguration.Rule()
//                    .withId("mu")
//                    .withAbortIncompleteMultipartUpload(
//                    new AbortIncompleteMultipartUpload()
//                        .withDaysAfterInitiation(1))))));
  }

  @Override
  public Stream<String> list() throws IOException {
    return StreamSupport.stream(new Spliterator<String>() {
      ObjectListing listing = s3client.listObjects(bucket);
      Iterator<S3ObjectSummary> current = listing.getObjectSummaries().iterator();

      @Override
      public boolean tryAdvance(Consumer<? super String> action) {
        if (!current.hasNext() && listing.isTruncated()) {
          listing = s3client.listNextBatchOfObjects(listing);
          current = listing.getObjectSummaries().iterator();
        }
        if (current.hasNext()) {
          action.accept(current.next().getKey());
          return true;
        } else {
          return false;
        }
      }

      @Override
      public Spliterator<String> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return DISTINCT | NONNULL | IMMUTABLE;
      }
    }, false);
  }

  /**
   * Do a multipart upload.
   * The length of the <code>buffer</code> array specifies the part size.
   * @param name the key to upload
   * @param buffer a fully-filled byte array with the first chunk of data
   *               (NOTE: this procedure modifies <code>buffer</code> in-place)
   * @param stream a stream with the rest of the data
   * @throws IOException
   */
  private void doMultipartUpload(String name, byte[] buffer, InputStream stream) throws IOException {
    // Ugh, wish the API would do this multipart nonsense for us.
    InitiateMultipartUploadResult result = s3client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, name));
    String uploadId = result.getUploadId();

    int partNumber = 1;
    long total = 0;
    int n = buffer.length;
    List<UploadPartResult> partResults = new ArrayList<>();
    do {
      // TODO: partNumber cannot be bigger than 10000!
      // TODO: checksums?
      UploadPartResult uploadPartResult = s3client.uploadPart(new UploadPartRequest()
              .withBucketName(bucket)
              .withKey(name)
              .withPartSize(n)
              .withFileOffset(total)
              .withPartNumber(partNumber)
              .withInputStream(new ByteArrayInputStream(buffer, 0, n))
              .withUploadId(uploadId));
      ++partNumber;
      total += n;
      partResults.add(uploadPartResult);
      n = Util.readChunk(stream, buffer);
    } while (n > 0);

    s3client.completeMultipartUpload(new CompleteMultipartUploadRequest()
            .withBucketName(bucket)
            .withKey(name)
            .withUploadId(uploadId)
            .withPartETags(partResults));
  }

  @Override
  public void createOrReplace(String name, InputStream stream) throws IOException {
    byte[] buffer = new byte[AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK];
    int n = Util.readChunk(stream, buffer);
    if (n < buffer.length) {
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(n);
      metadata.setContentType("application/octet-stream");
      s3client.putObject(bucket, name, new ByteArrayInputStream(buffer, 0, n), metadata);
    } else {
      doMultipartUpload(name, buffer, stream);
    }
  }

  @Override
  public OutputStream createOrReplace(String name) throws IOException {
//    // TODO: this operation is NOT consistent with open()
//    // New object creation works as expected, but object overwrites in S3
//    // are eventually consistent.  That means calling open() on the same
//    // name after invoking this method may give back stale data.  :(
//
//    // I'd like to stream directly to S3, but unfortunately the API requires that it
//    // know the exact size of the data being uploaded, which might not be possible
//    // for compressed streams and the like.
//    Path tmp = Files.createTempFile("s3upload", "");
//    return new FileOutputStream(tmp.toAbsolutePath().toString()) {
//      @Override
//      public void close() throws IOException {
//        super.close();
//        try {
//          s3client.putObject(bucket, name, tmp.toFile());
//        } finally {
//          Files.deleteIfExists(tmp);
//        }
//      }
//    };
    throw new UnsupportedOperationException();
  }

  @Override
  public InputStream open(String name) throws IOException {
    return s3client.getObject(bucket, name).getObjectContent();
  }

  @Override
  public void delete(String name) throws IOException {
    s3client.deleteObject(bucket, name);
  }

}
