package com.dynatrace.opentelemetry.metric;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;

public class OneAgentMetadataEnricherTest {
  Logger logger = Logger.getLogger(getClass().getName());
  OneAgentMetadataEnricher enricher = new OneAgentMetadataEnricher(logger);

  @Test
  public void validMetrics() {
    ArrayList<AbstractMap.SimpleEntry<String, String>> entries =
        new ArrayList<>(
            enricher.parseOneAgentMetadata(Arrays.asList("prop.a=value.a", "prop.b=value.b")));

    assertEquals("prop.a", entries.get(0).getKey());
    assertEquals("value.a", entries.get(0).getValue());
    assertEquals("prop.b", entries.get(1).getKey());
    assertEquals("value.b", entries.get(1).getValue());
  }

  @Test
  public void invalidMetrics() {
    assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("=0x5c14d9a68d569861")).isEmpty());
    assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("key_no_value=")).isEmpty());
    assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("===============")).isEmpty());
    assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("")).isEmpty());
    assertTrue(enricher.parseOneAgentMetadata(Arrays.asList("=")).isEmpty());
    assertTrue(enricher.parseOneAgentMetadata(Collections.emptyList()).isEmpty());
  }

  @Test
  public void testGetIndirectionFileContentValid() throws IOException {
    String expected = "dt_metadata_e617c525669e072eebe3d0f08212e8f2_private_target_file_specifier";
    // "mock" the contents of dt_metadata_e617c525669e072eebe3d0f08212e8f2.properties
    StringReader reader = new StringReader(expected);
    String result =
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2");
    assertEquals(expected, result);
  }

  @Test
  public void testGetIndirectionFileContentPassNullPrefix() throws IOException {
    String expected = "dt_metadata_e617c525669e072eebe3d0f08212e8f2_private_target_file_specifier";
    StringReader reader = new StringReader(expected);
    String result = enricher.getIndirectionFilename(reader, null);
    assertEquals(expected, result);
  }

  @Test
  public void testGetIndirectionFilePrefixInString() throws IOException {
    String input =
        "some_other_prefix_dt_metadata_e617c525669e072eebe3d0f08212e8f2_private_target_file_specifier";
    StringReader reader = new StringReader(input);
    assertNull(
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2"));
  }

  @Test
  public void testGetIndirectionFilePassNull() {
    assertThrows(IOException.class, () -> enricher.getIndirectionFilename(null, "prefix"));
  }

  @Test
  public void testGetIndirectionFileContentMissingPrefix() throws IOException {
    String expected = "private_target_file_specifier";
    StringReader reader = new StringReader(expected);
    assertNull(
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2"));
  }

  @Test
  public void testGetIndirectionFileContentEmptyContent() throws IOException {
    StringReader reader = new StringReader("");
    assertNull(
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2"));
  }

  @Test
  public void testGetIndirectionFilePassEmpty() throws IOException {
    StringReader reader = new StringReader("");
    assertNull(
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2"));
  }

  @Test
  public void testGetIndirectionFileContentEmptyLines() throws IOException {
    String expected = "dt_metadata_e617c525669e072eebe3d0f08212e8f2_private_target_file_specifier";
    String input = "this\nis\nirrelevant\n" + expected + "\neven\nmore\nirrelevant stuff\n\n\n";
    StringReader reader = new StringReader(input);
    String result =
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2");
    assertEquals(expected, result);
  }

  @Test
  public void testGetIndirectionFileContentSurroundingWhitespace() throws IOException {
    String expected = "dt_metadata_e617c525669e072eebe3d0f08212e8f2_private_target_file_specifier";
    String input = "    \n    " + expected + "\t   \n   ";
    StringReader reader = new StringReader(input);
    String result =
        enricher.getIndirectionFilename(reader, "dt_metadata_e617c525669e072eebe3d0f08212e8f2");
    assertEquals(expected, result);
  }

  @Test
  public void testGetOneAgentMetadataFileContentValid() throws IOException {
    List<String> expected = new ArrayList<>();
    expected.add("key1=value1");
    expected.add("key2=value2");
    expected.add("key3=value3");

    StringReader reader = new StringReader(String.join("\n", expected));
    List<String> result = enricher.getOneAgentMetadataFileContent(reader);
    assertEquals(expected, result);
    assertNotSame(expected, result);
  }

  @Test
  public void testGetOneAgentMetadataFileContentInvalid() throws IOException {
    List<String> inputs = Arrays.asList("=0", "", "a=", "\t\t", "=====", "    ", "   test   ");
    List<String> expected = Arrays.asList("=0", "", "a=", "", "=====", "", "test");

    StringReader reader = new StringReader(String.join("\n", inputs));
    List<String> result = enricher.getOneAgentMetadataFileContent(reader);
    assertEquals(expected, result);
    assertNotSame(expected, result);
  }

  @Test
  public void testGetOneAgentMetadataFileContentEmptyFile() throws IOException {
    List<String> expected = new ArrayList<>();

    List<String> result = enricher.getOneAgentMetadataFileContent(new StringReader(""));
    assertEquals(expected, result);
    assertNotSame(expected, result);
  }

  @Test
  public void testGetOneAgentMetadataFileContentPassNull() {
    assertThrows(IOException.class, () -> enricher.getOneAgentMetadataFileContent(null));
  }

  @Test
  public void testGetMetadataFileContentWithRedirection_Valid() {
    List<String> expected = Arrays.asList("key1=value1", "key2=value2", "key3=value3");
    List<String> results =
        enricher.getMetadataFileContentWithRedirection("src/test/resources/indirection");
    assertEquals(expected, results);
    assertNotSame(expected, results);
  }

  private String generateNonExistentFilename() {
    File f = null;
    Random r = new Random();
    // generate random filenames until we find one that does not exist:
    do {
      byte[] array = new byte[7];
      r.nextBytes(array);
      String filename = "src/test/resources/" + new String(array, StandardCharsets.UTF_8);

      f = new File(filename);
    } while (f.exists());
    return f.getAbsolutePath();
  }

  @Test
  public void testGetMetadataFileContentWithRedirection_IndirectionFileDoesNotExist() {
    String filename = generateNonExistentFilename();

    List<String> result = enricher.getMetadataFileContentWithRedirection(filename);
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedirection_IndirectionFileReturnsNull()
      throws Exception {
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    // ignore the return value of the testfile and mock the return value of the
    // getIndirectionFileName call:
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenReturn(null);
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedirection_IndirectionFileReturnsEmpty()
      throws Exception {
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    // ignore the return value of the testfile and mock the return value of the
    // getIndirectionFileName call:
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenReturn("");
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedirection_IndirectionFileThrows() throws Exception {
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    // in this case, the logger needs to be set on the mock, otherwise the logging call will throw a
    // NullReferenceException
    FieldSetter.setField(mockEnricher, mockEnricher.getClass().getDeclaredField("logger"), logger);
    // ignore the return value of the testfile and mock the return value of the
    // getIndirectionFileName call:
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenThrow(new IOException("test exception"));
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedireection_MetadataFileDoesNotExist()
      throws IOException, NoSuchFieldException {
    String metadataFilename = generateNonExistentFilename();
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    FieldSetter.setField(mockEnricher, mockEnricher.getClass().getDeclaredField("logger"), logger);
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenReturn(metadataFilename);
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedireection_MetadataFileReadThrows()
      throws IOException, NoSuchFieldException {
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    FieldSetter.setField(mockEnricher, mockEnricher.getClass().getDeclaredField("logger"), logger);
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenThrow(new IOException("test exception"));
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }

  @Test
  public void testGetMetadataFileContentWithRedireection_EmptyMetadataFile() throws IOException {
    OneAgentMetadataEnricher mockEnricher = Mockito.mock(OneAgentMetadataEnricher.class);
    Mockito.when(
            mockEnricher.getIndirectionFilename(Mockito.any(FileReader.class), Mockito.anyString()))
        .thenReturn("src/test/resources/mock_target.properties");
    Mockito.when(mockEnricher.getOneAgentMetadataFileContent(Mockito.any(FileReader.class)))
        .thenReturn(Collections.emptyList());
    Mockito.when(mockEnricher.getMetadataFileContentWithRedirection(Mockito.anyString()))
        .thenCallRealMethod();

    List<String> result =
        mockEnricher.getMetadataFileContentWithRedirection("src/test/resources/mock_target");
    assertEquals(Collections.<String>emptyList(), result);
  }
}
