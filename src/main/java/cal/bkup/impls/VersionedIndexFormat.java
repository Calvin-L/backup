package cal.bkup.impls;

import cal.bkup.types.IndexFormat;
import cal.prim.MalformedDataException;
import cal.prim.NonClosingInputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

/**
 * A <code>VersionedIndexFormat</code> allows migration from one index format
 * to another.  It stores a version number in the serialized backup index and
 * uses that to determine how to deserialize the index.  Serialized indexes
 * that do not have a version number are deserialized using the oldest format.
 * This class always serializes indexes using the newest format.
 *
 * <p>As long as all backup indexes are written through the oldest format or
 * through this class, it is possible to add new index formats without
 * worrying about backwards compatibility:
 * <pre>
 *   IndexFormat format = new VersionedIndexFormat(
 *       // Some index may have been written using one of these old formats.
 *       // Therefore:
 *       //   - Never change the implementations of the old formats.
 *       //   - Do not remove or reorder the old formats.
 *       new JsonIndexFormatV01(),
 *       ...
 *       // Add new formats here, at the end of the list
 *       );
 * </pre>
 *
 * <p>This class assumes that the oldest format never produces a serialized
 * index that starts with the byte <code>0xFF</code>.  The oldest index format
 * I wrote is {@link JsonIndexFormatV01}, which satisfies this constraint.
 *
 * @see #VersionedIndexFormat(IndexFormat...)
 */
public class VersionedIndexFormat implements IndexFormat {

  // INTERNAL DETAILS
  //
  // This format consists of a five-byte version header, followed by the
  // actual index data.  The version header has a single prefix byte (0xFF)
  // followed by a 4-byte big-endian version number.
  //
  // During deserialization, if the first byte is not 0xFF, then the first
  // format is used, on the assumption that the index was generated before
  // this versioned format was in use.

  private static final byte PREFIX = (byte)0xFF;

  private final IndexFormat[] formats;

  /**
   * Construct a versioned format using the given formats, ordered from oldest
   * to newest.
   *
   * @param formats the known formats
   */
  public VersionedIndexFormat(IndexFormat... formats) {
    if (formats.length == 0) {
      throw new IllegalArgumentException("VersionedIndexFormat requires at least one format");
    }
    this.formats = formats;
  }

  private IndexFormat oldestFormat() {
    return formats[0];
  }

  private int numberOfNewestFormat() {
    return formats.length - 1;
  }

  private IndexFormat newestFormat() {
    return formats[numberOfNewestFormat()];
  }

  private static int byteToUnsignedInt(byte b) {
    return ((int)b) & 0xFF;
  }

  static int readBigEndianInt(InputStream data) throws IOException, MalformedDataException {
    byte[] buffer = new byte[4];
    int nread = 0;
    while (nread < buffer.length) {
      int n = data.read(buffer, nread, buffer.length - nread);
      if (n < 0) {
        throw new MalformedDataException("Not enough bytes in the input stream");
      }
      nread += n;
    }
    return (byteToUnsignedInt(buffer[0]) << 24)
            | (byteToUnsignedInt(buffer[1]) << 16)
            | (byteToUnsignedInt(buffer[2]) << 8)
            | byteToUnsignedInt(buffer[3]);
  }

  static void serializeBigEndianInt(int i, byte[] buffer, int offset) {
    buffer[offset] = (byte)(i >> 24);
    buffer[offset + 1] = (byte)(i >> 16);
    buffer[offset + 2] = (byte)(i >> 8);
    buffer[offset + 3] = (byte)i;
  }

  private static byte[] createHeader(int version) {
    byte[] header = new byte[5];
    header[0] = PREFIX;
    serializeBigEndianInt(version, header, 1);
    return header;
  }

  @Override
  public BackupIndex load(InputStream data) throws IOException, MalformedDataException {
    int firstByte = data.read();
    if (firstByte < 0) {
      // No prefix byte?  Must have been an older format.  Let it decide what
      // to do with an empty input stream.
      return oldestFormat().load(data);
    }

    if ((byte)firstByte == PREFIX) {
      // We can load from a particular version.
      int version = readBigEndianInt(data);
      return formats[version].load(data);
    } else {
      // The very first index format did not support versioning.  Put back the
      // byte we read and send the whole buffer to the oldest version.
      //
      // The documentation for SequenceInputStream does not promise to keep the
      // final stream open until it is closed, and therefore may close it as the
      // last byte is consumed.  Since this procedure is expected NOT to close
      // the data stream, we first wrap it in a NonClosingInputStream.
      try (ByteArrayInputStream firstByteStream = new ByteArrayInputStream(new byte[] { (byte)firstByte });
           NonClosingInputStream restStream = new NonClosingInputStream(data);
           InputStream sequence = new SequenceInputStream(firstByteStream, restStream)) {
        return oldestFormat().load(sequence);
      }
    }
  }

  @Override
  public InputStream serialize(BackupIndex index) {
    ByteArrayInputStream headerStream = new ByteArrayInputStream(createHeader(numberOfNewestFormat()));
    try {
      InputStream indexStream = newestFormat().serialize(index);
      try {
        return new SequenceInputStream(headerStream, indexStream);
      } catch (Exception e) {
        try {
          indexStream.close();
        } catch (Exception onClose) {
          e.addSuppressed(onClose);
        }
        throw e;
      }
    } catch (Exception e) {
      try {
        headerStream.close();
      } catch (Exception onClose) {
        e.addSuppressed(onClose);
      }
      throw e;
    }
  }

}
