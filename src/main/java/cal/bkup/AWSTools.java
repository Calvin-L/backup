package cal.bkup;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class AWSTools {

  public static final int BYTES_PER_MULTIPART_UPLOAD_CHUNK = 128 * 1024 * 1024; // 128 Mb

  private static AWSCredentials credentials = null;

  public static synchronized AWSCredentials getCredentials() {
    if (credentials == null) {
      credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials();
    }
    return credentials;
  }

  public static AWSCredentialsProvider credentialsProvider() {
    return new AWSCredentialsProvider() {
      @Override
      public AWSCredentials getCredentials() {
        return AWSTools.getCredentials();
      }

      @Override
      public void refresh() {
      }
    };
  }

}
