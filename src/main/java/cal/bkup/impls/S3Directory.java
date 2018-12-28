package cal.bkup.impls;

import cal.bkup.AWSTools;
import cal.bkup.types.SimpleDirectory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class S3Directory implements SimpleDirectory {

  private final String bucket;
  private final AmazonS3 s3client;

  public S3Directory(String bucketName, String endpoint) {
    bucket = bucketName;
    s3client = new AmazonS3Client(AWSTools.getCredentials());
    s3client.setEndpoint(endpoint);
    if (!s3client.doesBucketExist(bucketName)) {
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

  @Override
  public OutputStream createOrReplace(String name) throws IOException {
    // TODO: this operation is NOT consistent with open()
    // New object creation works as expected, but object overwrites in S3
    // are eventually consistent.  That means calling open() on the same
    // name after invoking this method may give back stale data.  :(

    // I'd like to stream directly to S3, but unfortunately the API requires that it
    // know the exact size of the data being uploaded, which might not be possible
    // for compressed streams and the like.
    Path tmp = Files.createTempFile("s3upload", "");
    return new FileOutputStream(tmp.toAbsolutePath().toString()) {
      @Override
      public void close() throws IOException {
        super.close();
        try {
          s3client.putObject(bucket, name, tmp.toFile());
        } finally {
          Files.deleteIfExists(tmp);
        }
      }
    };
  }

  @Override
  public InputStream open(String name) throws IOException {
    return s3client.getObject(bucket, name).getObjectContent();
  }
}
