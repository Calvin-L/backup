package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.IndexFormat;
import cal.prim.MalformedDataException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

@SuppressWarnings("required.method.not.called")
public class JsonIndexFormatV02Tests {

  private final IndexFormat FORMAT = new JsonIndexFormatV02();

  @Test
  public void testLatestVersion() throws IOException, MalformedDataException {
    BackupIndex index = VersionedIndexFormatTests.createTestIndex();
    byte[] serialized = Util.read(FORMAT.serialize(index));
    BackupIndex deserialized = FORMAT.load(new ByteArrayInputStream(serialized));
    Assert.assertEquals(deserialized, index);
  }

}
