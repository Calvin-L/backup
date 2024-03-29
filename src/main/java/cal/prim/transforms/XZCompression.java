package cal.prim.transforms;

import cal.bkup.Util;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class XZCompression implements BlobTransformer {

  private final LZMA2Options options = new LZMA2Options();

  @Override
  @SuppressWarnings("mustcallalias.out.of.scope") // TODO?
  public @MustCallAlias InputStream apply(@MustCallAlias InputStream data) {
    return Util.createInputStream(os -> {
      try (OutputStream out = new XZOutputStream(os, options);
           InputStream copy = data /* ensure data gets closed */) {
        Util.copyStream(copy, out);
      }
    });
  }

  @Override
  public @MustCallAlias InputStream unApply(@MustCallAlias InputStream data) throws IOException {
    return new XZInputStream(data);
  }

}
