package ru.ing.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;

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
    void testProcessDataSimpleGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTest7zFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"3\""
        ));

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 1"));
        assertTrue(content.contains("Группа 1:"));
        assertTrue(content.contains("1;2;3"));
        assertTrue(content.contains("1;5;6"));
        assertTrue(content.contains("7;8;3"));
    }

    @Test
    void testProcessDataNoGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTest7zFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"4\";\"5\";\"6\"",
                "\"7\";\"8\";\"9\""
        ));

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 0"));
        assertFalse(content.contains("Группа 1:"));
    }

    @Test
    void testProcessDataWithDuplicates(@TempDir Path tempDir) throws IOException {
        File testFile = createTest7zFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"1\";\"2\";\"3\"",
                "\"4\";\"5\";\"6\""
        ));

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 1"));
    }

    @Test
    void testProcessDataComplexGroups(@TempDir Path tempDir) throws IOException {
        File testFile = createTest7zFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"9\"",
                "\"10\";\"8\";\"11\""
        ));

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 2"));
    }

    @Test
    void testProcessDataWithInvalidLines(@TempDir Path tempDir) throws IOException {
        File testFile = createTest7zFile(tempDir, Arrays.asList(
                "\"1\";\"2\";\"3\"",
                "invalid line without quotes",
                "\"1\";\"5\";\"6\"",
                "\"7\";\"8\";\"3\""
        ));

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 1"));
        assertTrue(content.contains("1;2;3"));
        assertTrue(content.contains("1;5;6"));
        assertTrue(content.contains("7;8;3"));
    }

    @Test
    void testProcessDataLargeGroup(@TempDir Path tempDir) throws IOException {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            lines.add("\"common\";\"" + i + "\";\"" + (i + 100) + "\"");
        }

        File testFile = createTest7zFile(tempDir, lines);

        File outputDir = tempDir.resolve("output").toFile();
        outputDir.mkdir();

        StringSearch search = new StringSearch() {
            @Override
            protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
                String filename = outputDir.getAbsolutePath() + "/output_" + dateFormat.format(new Date()) + ".txt";

                List<List<List<String>>> sortedGroups = new ArrayList<>(groups.values());
                sortedGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

                try (FileWriter writer = new FileWriter(filename)) {
                    long groupCount = sortedGroups.size();
                    System.out.println("Групп с более чем одной записью: " + groupCount);
                    writer.write("Групп с более чем одной записью: " + groupCount);
                    writer.write("\n-------------------------\n");

                    int groupNumber = 1;
                    for (List<List<String>> group : sortedGroups) {
                        writer.write("Группа " + groupNumber + ":\n");
                        for (List<String> record : group) {
                            writer.write(String.join(";", record) + "\n");
                        }
                        writer.write("\n");
                        groupNumber++;
                    }
                }
            }
        };

        search.processData(testFile.getAbsolutePath());

        File outputFile = findLatestOutputFile(outputDir);
        assertNotNull(outputFile, "Выходной файл не найден в " + outputDir);

        String content = readFileContent(outputFile);
        assertTrue(content.contains("Групп с более чем одной записью: 1"));
        for (int i = 0; i < 100; i++) {
            assertTrue(content.contains("common;" + i + ";" + (i + 100)));
        }
    }

    private File createTest7zFile(Path tempDir, List<String> lines) throws IOException {
        File sevenZFile = tempDir.resolve("test.7z").toFile();

        try (SevenZOutputFile sevenZOutput = new SevenZOutputFile(sevenZFile)) {
            SevenZArchiveEntry entry = sevenZOutput.createArchiveEntry(
                    sevenZFile,
                    "test.csv"
            );
            sevenZOutput.putArchiveEntry(entry);

            StringBuilder content = new StringBuilder();
            for (String line : lines) {
                content.append(line).append("\n");
            }

            sevenZOutput.write(content.toString().getBytes(StandardCharsets.UTF_8));
            sevenZOutput.closeArchiveEntry();
        }

        return sevenZFile;
    }

    private File findLatestOutputFile(File directory) {
        File[] files = directory.listFiles((dir, name) -> name.startsWith("output_") && name.endsWith(".txt"));
        if (files == null || files.length == 0) return null;

        return Arrays.stream(files)
                .max(Comparator.comparingLong(File::lastModified))
                .orElse(null);
    }

    private String readFileContent(File file) throws IOException {
        StringBuilder content = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                content.append(line).append("\n");
            }
        }
        return content.toString();
    }
}