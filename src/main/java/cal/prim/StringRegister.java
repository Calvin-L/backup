package cal.prim;

import java.io.IOException;

public interface StringRegister {

  String read() throws IOException;
  void write(String previous, String next) throws IOException, PreconditionFailed;

}
