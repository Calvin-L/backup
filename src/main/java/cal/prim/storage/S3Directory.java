package cal.prim.storage;

import cal.bkup.AWSTools;
import cal.bkup.Util;
import org.checkerframework.checker.interning.qual.UnknownInterned;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.regex.qual.UnknownRegex;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3Directory implements EventuallyConsistentDirectory {

  private final String bucket;
  private final S3Client s3client;

  public S3Directory(S3Client s3client, String bucketName) {
    this.bucket = bucketName;
    this.s3client = s3client;
    try {
      s3client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
    } catch (NoSuchBucketException e) {
      s3client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
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
    ListObjectsV2Request request =
              ListObjectsV2Request.builder()
                      .bucket(bucket)
                      .build();
    ListObjectsV2Response initialListing;
    try {
      initialListing = s3client.listObjectsV2(request);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }

    return StreamSupport.stream(new Spliterator<>() {
      ListObjectsV2Response listing = initialListing;
      Iterator<S3Object> current = listing.contents().iterator();

      @Override
      public boolean tryAdvance(Consumer<? super @UnknownInterned @UnknownRegex String> action) {
        if (!current.hasNext() && listing.isTruncated()) {
          listing = s3client.listObjectsV2(request.toBuilder()
                  .continuationToken(listing.continuationToken())
                  .build());
          current = listing.contents().iterator();
        }
        if (current.hasNext()) {
          action.accept(current.next().key());
          return true;
        } else {
          return false;
        }
      }

      @Override
      public @Nullable Spliterator<String> trySplit() {
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
    CreateMultipartUploadResponse result = s3client.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(bucket).key(name).build());
    String uploadId = result.uploadId();

    int partNumber = 1;
    long total = 0;
    int n = buffer.length;
    List<UploadPartResponse> partResults = new ArrayList<>();
    do {
      // TODO: partNumber cannot be bigger than 10000!
      // TODO: checksums?
      UploadPartResponse uploadPartResult = s3client.uploadPart(
              UploadPartRequest.builder()
                      .bucket(bucket)
                      .key(name)
                      .partNumber(partNumber)
                      .uploadId(uploadId)
                      .build(),
              RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, n)));
      ++partNumber;
      total += n;
      partResults.add(uploadPartResult);
      n = Util.readChunk(stream, buffer);
    } while (n > 0);

    s3client.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
            .bucket(bucket)
            .key(name)
            .uploadId(uploadId)
            .build());
  }

  @Override
  public void createOrReplace(String name, InputStream stream) throws IOException {
    try {
      byte[] buffer = new byte[AWSTools.BYTES_PER_MULTIPART_UPLOAD_CHUNK];
      int n = Util.readChunk(stream, buffer);
      if (n < buffer.length) {
        s3client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(name)
//                        .contentLength((long)n)
//                        .contentType("application/octet-stream")
                        .build(),
                RequestBody.fromByteBuffer(ByteBuffer.wrap(buffer, 0, n)));
      } else {
        doMultipartUpload(name, buffer, stream);
      }
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream open(String name) throws IOException {
    try {
      return s3client.getObject(
              GetObjectRequest.builder()
                      .bucket(bucket)
                      .key(name)
                      .build());
    } catch (NoSuchKeyException e) {
      throw new NoSuchFileException(name);
    } catch (SdkClientException e) {
      // "SdkClientException" indicates any other kind of error, including:
      //   - malformed request
      //   - network error
      //   - unable to parse response from Amazon
      throw new IOException(e);
    }
  }

  @Override
  public void delete(String name) throws IOException {
    try {
      s3client.deleteObject(
              DeleteObjectRequest.builder()
                      .bucket(bucket)
                      .key(name)
                      .build());
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

}
