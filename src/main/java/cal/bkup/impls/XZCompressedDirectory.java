package cal.bkup.impls;

import cal.bkup.types.SimpleDirectory;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZInputStream;
import org.tukaani.xz.XZOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.stream.Stream;

public class XZCompressedDirectory implements SimpleDirectory {

  private final SimpleDirectory wrapped;

  public XZCompressedDirectory(SimpleDirectory wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public Stream<String> list() throws IOException {
    return wrapped.list();
  }

  @Override
  public OutputStream create(String name) throws IOException {
    return new XZOutputStream(wrapped.create(name), new LZMA2Options());
  }

  @Override
  public OutputStream createOrReplace(String name) throws IOException {
    return new XZOutputStream(wrapped.createOrReplace(name), new LZMA2Options());
  }

  @Override
  public InputStream open(String name) throws IOException {
    return new XZInputStream(wrapped.open(name));
  }

}
