package cal.bkup;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

public class AWSTools {

  public static final int BYTES_PER_MULTIPART_UPLOAD_CHUNK = 128 * 1024 * 1024; // 128 Mb

  public static AwsCredentialsProvider credentialsProvider() {
    return DefaultCredentialsProvider.create();
  }

}
