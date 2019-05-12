package cal.bkup;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class AWSTools {

  public static final int BYTES_PER_MULTIPART_UPLOAD_CHUNK = 128 * 1024 * 1024; // 128 Mb

  public static AWSCredentialsProvider credentialsProvider() {
    return DefaultAWSCredentialsProviderChain.getInstance();
  }

}
