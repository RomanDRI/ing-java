package ru.ing.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;

class StringSearchTest {

    @Test
    void testIsValidLine() {

        assertTrue(StringSearch.isValidLine("\"a\";\"b\";\"c\""));
        assertTrue(StringSearch.isValidLine("\"a\";\"\";\"c\""));
        assertTrue(StringSearch.isValidLine("\"a\""));
        assertTrue(StringSearch.isValidLine("\"a\";\"b\""));

        assertFalse(StringSearch.isValidLine("\"a\"b\";\"c\""));
        assertFalse(StringSearch.isValidLine("a;b;c"));
        assertFalse(StringSearch.isValidLine("\"a\";b;\"c\""));
        assertFalse(StringSearch.isValidLine("\"a\";\"b\";\"c"));
    }

    @Test
    void testGroupRecords() {

        List<List<String>> records1 = Arrays.asList(
                Arrays.asList("1", "2", "3"),
                Arrays.asList("1", "5", "6"),
                Arrays.asList("7", "8", "3")
        );

        List<Set<List<String>>> groups1 = StringSearch.groupRecords(records1);
        assertEquals(1, groups1.size());

        List<List<String>> records2 = Arrays.asList(
                Arrays.asList("1", "2", "3"),
                Arrays.asList("4", "5", "6"),
                Arrays.asList("7", "8", "9")
        );

        List<Set<List<String>>> groups2 = StringSearch.groupRecords(records2);
        assertEquals(3, groups2.size());

        List<List<String>> records3 = Arrays.asList(
                Arrays.asList("1", "2", "3"),
                Arrays.asList("1", "5", "6"),
                Arrays.asList("7", "8", "9"),
                Arrays.asList("10", "8", "11")
        );

        List<Set<List<String>>> groups3 = StringSearch.groupRecords(records3);
        assertEquals(2, groups3.size());
    }

    @Test
    void testReadDataFromFile(@TempDir Path tempDir) throws IOException {
        File testFile = tempDir.resolve("test.csv.gz").toFile();

        try (OutputStream out = new FileOutputStream(testFile);
             GZIPOutputStream gzipOut = new GZIPOutputStream(out);
             Writer writer = new OutputStreamWriter(gzipOut)) {

            writer.write("\"1\";\"2\";\"3\"\n");
            writer.write("\"4\";\"5\";\"6\"\n");
            writer.write("\"1\";\"2\";\"3\"\n");
            writer.write("invalid line\n");
        }

        List<List<String>> records = StringSearch.readDataFromFile(testFile.getAbsolutePath());

        assertEquals(2, records.size());
        assertEquals(Arrays.asList("1", "2", "3"), records.get(0));
        assertEquals(Arrays.asList("4", "5", "6"), records.get(1));
    }
}