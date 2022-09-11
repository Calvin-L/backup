package cal.bkup.impls;

import cal.bkup.Util;
import cal.bkup.types.IndexFormat;
import cal.bkup.types.SystemId;
import cal.prim.MalformedDataException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

@Test
public class JsonIndexFormatV01Tests {

  private final IndexFormat FORMAT = new JsonIndexFormatV01();

  private final String TEST_INDEX = "{\n" +
          "  \"files\": {\n" +
          "    \"UWLaptop2\": {\n" +
          "      \"/Users/loncaric/Pictures/Photos Library.photoslibrary/Masters/2018/09/24/20180924-035557/IMG_7645.JPG\": [\n" +
          "        {\n" +
          "          \"modTimeAsMillisecondsSinceEpoch\": 1497000673000,\n" +
          "          \"sha256\": \"403e6a712c5854ead45c0a57bf56e0a7280237c67b4c68bd0f464e228df046f8\",\n" +
          "          \"size\": 5433228,\n" +
          "          \"symLinkDst\": null,\n" +
          "          \"hardLinkDst\": null\n" +
          "        }\n" +
          "      ],\n" +
          "      \"/Users/loncaric/Pictures/Photos Library.photoslibrary/resources/proxies/derivatives/15/00/15ce/UNADJUSTEDNONRAW_mini_15ce.jpg\": [\n" +
          "        {\n" +
          "          \"modTimeAsMillisecondsSinceEpoch\": 1553735880000,\n" +
          "          \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
          "          \"size\": 92077,\n" +
          "          \"symLinkDst\": null,\n" +
          "          \"hardLinkDst\": null\n" +
          "        },\n" +
          "        {\n" +
          "          \"modTimeAsMillisecondsSinceEpoch\": 1553735880030,\n" +
          "          \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
          "          \"size\": 92077,\n" +
          "          \"symLinkDst\": null,\n" +
          "          \"hardLinkDst\": null\n" +
          "        }\n" +
          "      ],\n" +
          "      \"/Users/loncaric/src/backup/config.json\": [\n" +
          "        {\n" +
          "          \"modTimeAsMillisecondsSinceEpoch\": null,\n" +
          "          \"sha256\": null,\n" +
          "          \"size\": 0,\n" +
          "          \"symLinkDst\": \"/Users/loncaric/.backup-config.json\",\n" +
          "          \"hardLinkDst\": null\n" +
          "        }\n" +
          "      ]\n" +
          "    }\n" +
          "  },\n" +
          "  \"blobs\": [\n" +
          "    {\n" +
          "      \"key\": \"???\",\n" +
          "      \"sha256\": \"403e6a712c5854ead45c0a57bf56e0a7280237c67b4c68bd0f464e228df046f8\",\n" +
          "      \"size\": 5433228,\n" +
          "      \"backupId\": \"Vnubxqb8V9yBO490Me8yU6SRZ3BzOuiTj9Zr2FZU7AH-cO2EBNvx8Q0lTo47MbzXnVmeJ4dJHiQWjWfqtHOJXIf8Io-ebqiQ7VYUh2ekG13awCPga-3Nt1xzglkDhQz4HGHc3xOqCw\",\n" +
          "      \"backupSize\": 5429272\n" +
          "    },\n" +
          "    {\n" +
          "      \"key\": \"???\",\n" +
          "      \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
          "      \"size\": 92077,\n" +
          "      \"backupId\": \"1dQ4JLD0XDNiEZh3dftDOa6MZ7pgAV1ykspkQZ8kE_pDd4_gAwp7O0LX5-TddaasUUsP3lNL8P54Mtxbq-gr1Gq19bUjUx4OJrurqBaLg9eqBmnJeJyYadvkG0UF-JCWKX43BNzS-g\",\n" +
          "      \"backupSize\": 92280\n" +
          "    }\n" +
          "  ]\n" +
          "}";

  @Test
  public void testRead() throws IOException, MalformedDataException {
    BackupIndex index = FORMAT.load(new ByteArrayInputStream(TEST_INDEX.getBytes(StandardCharsets.UTF_8)));

    SystemId system = new SystemId("UWLaptop2");
    Assert.assertEquals(index.knownSystems(), Collections.singleton(system));
    Assert.assertEquals(index.knownPaths(system), new HashSet<>(Arrays.asList(
            Paths.get("/Users/loncaric/Pictures/Photos Library.photoslibrary/Masters/2018/09/24/20180924-035557/IMG_7645.JPG"),
            Paths.get("/Users/loncaric/Pictures/Photos Library.photoslibrary/resources/proxies/derivatives/15/00/15ce/UNADJUSTEDNONRAW_mini_15ce.jpg"),
            Paths.get("/Users/loncaric/src/backup/config.json"))));

    Assert.assertEquals(index.getInfo(system, Paths.get("/Users/loncaric/src/backup/config.json")), Collections.singletonList(
            new BackupIndex.SoftLinkRev(0, Paths.get("/Users/loncaric/.backup-config.json"))
    ));
  }

  @Test
  public void testRewrite() throws IOException, MalformedDataException {
    BackupIndex index1 = FORMAT.load(new ByteArrayInputStream(TEST_INDEX.getBytes(StandardCharsets.UTF_8)));
    byte[] reserialized = Util.read(FORMAT.serialize(index1));
    BackupIndex index2 = FORMAT.load(new ByteArrayInputStream(reserialized));
    Assert.assertEquals(index1, index2);
  }

  @Test
  public void testReadWithMissingFields() throws IOException, MalformedDataException {

    String text = "{\n" +
            "  \"files\": {\n" +
            "    \"UWLaptop2\": {\n" +
            "      \"/Users/loncaric/Pictures/Photos Library.photoslibrary/Masters/2018/09/24/20180924-035557/IMG_7645.JPG\": [\n" +
            "        {\n" +
            "          \"modTimeAsMillisecondsSinceEpoch\": 1497000673000,\n" +
            "          \"sha256\": \"403e6a712c5854ead45c0a57bf56e0a7280237c67b4c68bd0f464e228df046f8\",\n" +
            "          \"size\": 5433228\n" +
            "        }\n" +
            "      ],\n" +
            "      \"/Users/loncaric/Pictures/Photos Library.photoslibrary/resources/proxies/derivatives/15/00/15ce/UNADJUSTEDNONRAW_mini_15ce.jpg\": [\n" +
            "        {\n" +
            "          \"modTimeAsMillisecondsSinceEpoch\": 1553735880000,\n" +
            "          \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
            "          \"size\": 92077\n" +
            "        },\n" +
            "        {\n" +
            "          \"modTimeAsMillisecondsSinceEpoch\": 1553735880030,\n" +
            "          \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
            "          \"size\": 92077\n" +
            "        }\n" +
            "      ],\n" +
            "      \"/Users/loncaric/src/backup/config.json\": [\n" +
            "        {\n" +
            "          \"symLinkDst\": \"/Users/loncaric/.backup-config.json\"\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"blobs\": [\n" +
            "    {\n" +
            "      \"key\": \"???\",\n" +
            "      \"sha256\": \"403e6a712c5854ead45c0a57bf56e0a7280237c67b4c68bd0f464e228df046f8\",\n" +
            "      \"size\": 5433228,\n" +
            "      \"backupId\": \"Vnubxqb8V9yBO490Me8yU6SRZ3BzOuiTj9Zr2FZU7AH-cO2EBNvx8Q0lTo47MbzXnVmeJ4dJHiQWjWfqtHOJXIf8Io-ebqiQ7VYUh2ekG13awCPga-3Nt1xzglkDhQz4HGHc3xOqCw\",\n" +
            "      \"backupSize\": 5429272\n" +
            "    },\n" +
            "    {\n" +
            "      \"key\": \"???\",\n" +
            "      \"sha256\": \"1fb781ab98a654dd3c58e7e1ca80084b9244dd0cfd2a1a2dcd7c2d6476d82d6c\",\n" +
            "      \"size\": 92077,\n" +
            "      \"backupId\": \"1dQ4JLD0XDNiEZh3dftDOa6MZ7pgAV1ykspkQZ8kE_pDd4_gAwp7O0LX5-TddaasUUsP3lNL8P54Mtxbq-gr1Gq19bUjUx4OJrurqBaLg9eqBmnJeJyYadvkG0UF-JCWKX43BNzS-g\",\n" +
            "      \"backupSize\": 92280\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    BackupIndex index = FORMAT.load(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)));

    SystemId system = new SystemId("UWLaptop2");
    Assert.assertEquals(index.knownSystems(), Collections.singleton(system));
    Assert.assertEquals(index.knownPaths(system), new HashSet<>(Arrays.asList(
            Paths.get("/Users/loncaric/Pictures/Photos Library.photoslibrary/Masters/2018/09/24/20180924-035557/IMG_7645.JPG"),
            Paths.get("/Users/loncaric/Pictures/Photos Library.photoslibrary/resources/proxies/derivatives/15/00/15ce/UNADJUSTEDNONRAW_mini_15ce.jpg"),
            Paths.get("/Users/loncaric/src/backup/config.json"))));

    Assert.assertEquals(index.getInfo(system, Paths.get("/Users/loncaric/src/backup/config.json")), Collections.singletonList(
            new BackupIndex.SoftLinkRev(0, Paths.get("/Users/loncaric/.backup-config.json"))
    ));

  }

}
