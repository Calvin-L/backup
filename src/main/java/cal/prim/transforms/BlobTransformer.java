package cal.prim.transforms;

import java.io.IOException;
import java.io.InputStream;

public interface BlobTransformer {

  InputStream apply(InputStream data) throws IOException;
  InputStream unApply(InputStream data) throws IOException;

  default BlobTransformer followedBy(BlobTransformer next) {
    BlobTransformer self = this;
    return new BlobTransformer() {
      @Override
      public InputStream apply(InputStream data) throws IOException {
        return next.apply(self.apply(data));
      }

      @Override
      public InputStream unApply(InputStream data) throws IOException {
        return self.unApply(next.unApply(data));
      }
    };
  }
}
