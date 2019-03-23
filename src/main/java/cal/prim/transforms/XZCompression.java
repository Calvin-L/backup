package cal.prim.transforms;

import cal.bkup.Util;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.UnsupportedOptionsException;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class XZCompression implements BlobTransformer {

  private final LZMA2Options options;

  public XZCompression() {
    LZMA2Options options;
    try {
      options = new LZMA2Options(LZMA2Options.PRESET_MAX);
    } catch (UnsupportedOptionsException e) {
      System.err.println("XZ library does not support PRESET_MAX compression level!");
      System.err.println("Using the default compression level instead.");
      options = new LZMA2Options();
    }
    this.options = options;
  }

  @Override
  public InputStream apply(InputStream data) {
    return Util.createInputStream(os -> {
      try (OutputStream out = new XZOutputStream(os, options);
           InputStream copy = data /* ensure data gets closed */) {
        Util.copyStream(copy, out);
      }
    });
  }

  @Override
  public InputStream unApply(InputStream data) throws IOException {
    return new XZInputStream(data);
  }

}
