package cal.prim.transforms;

import org.checkerframework.checker.mustcall.qual.MustCallAlias;

import java.io.IOException;
import java.io.InputStream;

public interface BlobTransformer {

  @MustCallAlias InputStream apply(@MustCallAlias InputStream data) throws IOException;
  @MustCallAlias InputStream unApply(@MustCallAlias InputStream data) throws IOException;

  default BlobTransformer followedBy(BlobTransformer next) {
    BlobTransformer self = this;
    return new BlobTransformer() {
      @Override
      public @MustCallAlias InputStream apply(@MustCallAlias InputStream data) throws IOException {
        return next.apply(self.apply(data));
      }

      @Override
      public @MustCallAlias InputStream unApply(@MustCallAlias InputStream data) throws IOException {
        return self.unApply(next.unApply(data));
      }
    };
  }
}
