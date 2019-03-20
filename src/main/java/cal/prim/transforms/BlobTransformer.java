package cal.prim.transforms;

import java.io.IOException;
import java.io.InputStream;

public interface BlobTransformer {

  InputStream apply(InputStream data) throws IOException;
  InputStream unApply(InputStream data) throws IOException;

}
