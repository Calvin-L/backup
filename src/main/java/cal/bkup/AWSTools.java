package cal.bkup;

import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.Owning;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.utils.SdkAutoCloseable;

import java.io.Closeable;

public class AWSTools {

  public static final int BYTES_PER_MULTIPART_UPLOAD_CHUNK = 128 * 1024 * 1024; // 128 Mb

  public static CloseableCredentialsProvider credentialsProvider() {
    var credentials = DefaultCredentialsProvider.create();
    return new CredentialsProviderWithResource(credentials, credentials);
  }

  public interface CloseableCredentialsProvider extends AwsCredentialsProvider, Closeable {
    @Override
    void close();
  }

  private static class CredentialsProviderWithResource implements CloseableCredentialsProvider {

    private final AwsCredentialsProvider delegate;
    private final @Owning SdkAutoCloseable resource;

    public CredentialsProviderWithResource(AwsCredentialsProvider delegate, @Owning SdkAutoCloseable resource) {
      this.delegate = delegate;
      this.resource = resource;
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return delegate.resolveCredentials();
    }

    @Override
    @EnsuresCalledMethods(value = "resource", methods = {"close"})
    public void close() {
      resource.close();
    }
  }

}
