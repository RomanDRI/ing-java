package ru.ing.util;

import ru.ing.model.Key;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class StringSearch {

    public static final Pattern VALID_LINE_PATTERN = Pattern.compile("^(\"[^\"]*(\"{2}[^\"]*)*\";)*\"[^\"]*(\"{2}[^\"]*)*\"$");

    public void processData(String fileName) throws IOException {
        List<List<String>> records = readDataFromFile(fileName);
        List<Set<List<String>>> groups = groupRecords(records);
        writeResultsToFile(groups);
    }

    protected static List<List<String>> readDataFromFile(String fileName) throws IOException {
        List<List<String>> records = new ArrayList<>();

        try (FileInputStream fis = new FileInputStream(fileName);
             GZIPInputStream gzis = new GZIPInputStream(fis);
             InputStreamReader isr = new InputStreamReader(gzis);
             BufferedReader br = new BufferedReader(isr)) {

            String line;
            while ((line = br.readLine()) != null) {
                if (isValidLine(line)) {
                    List<String> recordValues = Arrays.asList(line.split(";"));
                    for (int i = 0; i < recordValues.size(); i++) {
                        String value = recordValues.get(i);
                        if (value.startsWith("\"") && value.endsWith("\"")) {
                            recordValues.set(i, value.substring(1, value.length() - 1));
                        }
                    }
                    records.add(recordValues);
                } else {
                    System.out.println("Некорректная строка пропущена: " + line);
                }
            }
        }

        return new ArrayList<>(new LinkedHashSet<>(records));
    }

    protected static boolean isValidLine(String line) {
        return VALID_LINE_PATTERN.matcher(line).matches();
    }

    protected static List<Set<List<String>>> groupRecords(List<List<String>> records) {
        List<Set<List<String>>> groups = new ArrayList<>();
        Map<Key, Set<List<String>>> valueToGroups = new HashMap<>();

        for (List<String> record : records) {
            Set<Set<List<String>>> matchingGroups = new HashSet<>();

            for (int i = 0; i < record.size(); i++) {
                String value = record.get(i);
                if (!value.isEmpty()) {
                    Key key = new Key(value, i);
                    if (valueToGroups.containsKey(key)) {
                        matchingGroups.add(valueToGroups.get(key));
                    }
                }
            }

            if (matchingGroups.isEmpty()) {
                Set<List<String>> newGroup = new HashSet<>();
                newGroup.add(record);
                groups.add(newGroup);
                for (int i = 0; i < record.size(); i++) {
                    String value = record.get(i);
                    if (!value.isEmpty()) {
                        Key key = new Key(value, i);
                        valueToGroups.put(key, newGroup);
                    }
                }
            } else {
                Set<List<String>> mergedGroup = matchingGroups.iterator().next();
                for (Set<List<String>> group : matchingGroups) {
                    if (group != mergedGroup) {
                        mergedGroup.addAll(group);
                        for (List<String> r : group) {
                            for (int i = 0; i < r.size(); i++) {
                                String value = r.get(i);
                                if (!value.isEmpty()) {
                                    valueToGroups.put(new Key(value, i), mergedGroup);
                                }
                            }
                        }
                        groups.remove(group);
                    }
                }
                mergedGroup.add(record);
                for (int i = 0; i < record.size(); i++) {
                    String value = record.get(i);
                    if (!value.isEmpty()) {
                        valueToGroups.put(new Key(value, i), mergedGroup);
                    }
                }
            }
        }

        return groups;
    }

    protected static void writeResultsToFile(List<Set<List<String>>> groups) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
        String filename = "output_" + dateFormat.format(new Date()) + ".txt";

        groups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

        try (FileWriter writer = new FileWriter(filename)) {
            long groupCount = groups.stream().filter(g -> g.size() > 1).count();
            System.out.println("Групп с более чем одной записью: " + groupCount);
            writer.write("Групп с более чем одной записью: " + groupCount);
            writer.write("\n-------------------------\n");

            int groupNumber = 1;
            for (Set<List<String>> group : groups) {
                if (group.size() > 1) {
                    writer.write("Группа " + groupNumber + ":\n");
                    for (List<String> record : group) {
                        String formattedRecord = String.join(";", record);
                        writer.write(formattedRecord + "\n");
                    }
                    writer.write("\n");
                    groupNumber++;
                }
            }
        }
    }
}