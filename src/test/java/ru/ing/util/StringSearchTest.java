package ru.ing.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;

class StringSearchTest {
    @Test
    void testSimpleGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"3\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(1, search.getGroupCount());
        assertEquals(3, search.getLargestGroupSize());
    }

    @Test
    void testNoGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"4\";\"5\";\"6\"",
                "\"7\";\"8\";\"9\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(0, search.getGroupCount());
    }

    @Test
    void testComplexGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"9\"",
                "\"10\";\"8\";\"11\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(2, search.getGroupCount());
    }

    @Test
    void testEmptyValues(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"\";\"3\"",
                "\"1\";\"5\";\"\"",
                "\"\";\"8\";\"3\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(1, search.getGroupCount());
    }

    @Test
    void testQuotedStrings(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2;3\";\"4\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"4\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(1, search.getGroupCount());
        assertEquals(3, search.getLargestGroupSize());
    }

    @Test
    void testDoubleQuotes(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2\"\"3\";\"4\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"4\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(1, search.getGroupCount());
    }

    @Test
    void testTransitiveConnections(@TempDir Path tempDir) throws IOException {
        File testFile = createTestCsvFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"4\";\"2\";\"5\"",
                "\"4\";\"6\";\"7\"",
                "\"8\";\"6\";\"9\""
        ));

        TestStringSearch search = new TestStringSearch();
        search.processData(testFile.getAbsolutePath());

        assertEquals(1, search.getGroupCount());
        assertEquals(4, search.getLargestGroupSize());
    }

    private File createTestCsvFile(Path tempDir, List<String> lines) throws IOException {
        File testFile = tempDir.resolve("test.csv").toFile();
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(testFile), StandardCharsets.UTF_8))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
        return testFile;
    }

    private static class TestStringSearch extends StringSearch {
        private int groupCount = 0;
        private int largestGroupSize = 0;

        @Override
        protected void writeResultsToFile(List<Set<String>> groups) {
            List<Set<String>> validGroups = new ArrayList<>();
            for (Set<String> group : groups) {
                if (group != null && group.size() > 1) {
                    validGroups.add(group);
                }
            }

            groupCount = validGroups.size();

            if (!validGroups.isEmpty()) {
                validGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));
                largestGroupSize = validGroups.get(0).size();
            }
        }

        public int getGroupCount() {
            return groupCount;
        }

        public int getLargestGroupSize() {
            return largestGroupSize;
        }
    }
}