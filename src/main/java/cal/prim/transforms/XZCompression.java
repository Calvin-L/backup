package cal.prim.transforms;

import cal.bkup.Util;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class XZCompression implements BlobTransformer {

  private final LZMA2Options options = new LZMA2Options();

  @Override
  public InputStream apply(InputStream data) throws IOException {
    return Util.createInputStream(os -> {
      try (OutputStream out = new XZOutputStream(os, options)) {
        Util.copyStream(data, out);
      }
    });
  }

  @Override
  public InputStream unApply(InputStream data) throws IOException {
    return new XZInputStream(data);
  }

}
