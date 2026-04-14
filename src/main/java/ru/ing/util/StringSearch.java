package ru.ing.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class StringSearch {

    public static final Pattern VALID_LINE_PATTERN =
            Pattern.compile("^(\"[^\"]*(\"{2}[^\"]*)*\";)*\"[^\"]*(\"{2}[^\"]*)*\"$");

    public void processData(String fileName) throws IOException {
        Map<String, Set<Integer>> columnValueToGroupIds = new HashMap<>();
        List<Set<List<String>>> groups = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))) {

            String line;
            while ((line = br.readLine()) != null) {
                if (isValidLine(line)) {
                    List<String> record = parseRecord(line);
                    processRecord(record, columnValueToGroupIds, groups);
                }
            }
        }

        writeResultsToFile(groups);
    }

    private void processRecord(List<String> record,
                               Map<String, Set<Integer>> columnValueToGroupIds,
                               List<Set<List<String>>> groups) {
        Set<Integer> matchingGroupIds = new HashSet<>();

        for (int colIndex = 0; colIndex < record.size(); colIndex++) {
            String value = record.get(colIndex);
            if (!value.isEmpty()) {
                String key = colIndex + ":" + value;
                Set<Integer> groupIds = columnValueToGroupIds.get(key);
                if (groupIds != null) {
                    matchingGroupIds.addAll(groupIds);
                }
            }
        }

        if (matchingGroupIds.isEmpty()) {
            Set<List<String>> newGroup = new HashSet<>();
            newGroup.add(record);
            groups.add(newGroup);
            int newGroupId = groups.size() - 1;

            addKeysForRecord(record, newGroupId, columnValueToGroupIds);
        } else {
            List<Integer> sortedGroupIds = new ArrayList<>(matchingGroupIds);
            sortedGroupIds.sort(Collections.reverseOrder());

            int mainGroupId = sortedGroupIds.get(0);
            Set<List<String>> mainGroup = groups.get(mainGroupId);
            mainGroup.add(record);

            for (int i = 1; i < sortedGroupIds.size(); i++) {
                int groupIdToMerge = sortedGroupIds.get(i);
                Set<List<String>> groupToMerge = groups.get(groupIdToMerge);
                mainGroup.addAll(groupToMerge);

                updateGroupReferences(groupToMerge, groupIdToMerge, mainGroupId, columnValueToGroupIds);

                groups.set(groupIdToMerge, null);
            }

            addKeysForRecord(record, mainGroupId, columnValueToGroupIds);
        }
    }

    private void addKeysForRecord(List<String> record, int groupId,
                                  Map<String, Set<Integer>> columnValueToGroupIds) {
        for (int colIndex = 0; colIndex < record.size(); colIndex++) {
            String value = record.get(colIndex);
            if (!value.isEmpty()) {
                String key = colIndex + ":" + value;
                columnValueToGroupIds
                        .computeIfAbsent(key, k -> new HashSet<>())
                        .add(groupId);
            }
        }
    }

    private void updateGroupReferences(Set<List<String>> group, int oldGroupId, int newGroupId,
                                       Map<String, Set<Integer>> columnValueToGroupIds) {
        for (List<String> record : group) {
            for (int colIndex = 0; colIndex < record.size(); colIndex++) {
                String value = record.get(colIndex);
                if (!value.isEmpty()) {
                    String key = colIndex + ":" + value;
                    Set<Integer> groupIds = columnValueToGroupIds.get(key);
                    if (groupIds != null) {
                        groupIds.remove(oldGroupId);
                        groupIds.add(newGroupId);
                    }
                }
            }
        }
    }

    protected List<String> parseRecord(String line) {
        String[] values = line.split(";");
        List<String> result = new ArrayList<>(values.length);
        for (String value : values) {
            if (value.startsWith("\"") && value.endsWith("\"")) {
                result.add(value.substring(1, value.length() - 1).replace("\"\"", "\""));
            } else {
                result.add(value);
            }
        }
        return result;
    }

    protected static boolean isValidLine(String line) {
        return VALID_LINE_PATTERN.matcher(line).matches();
    }

    protected void writeResultsToFile(List<Set<List<String>>> groups) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
        String filename = "output_" + dateFormat.format(new Date()) + ".txt";

        List<Set<List<String>>> validGroups = new ArrayList<>();
        for (Set<List<String>> group : groups) {
            if (group != null && group.size() > 1) {
                validGroups.add(group);
            }
        }

        validGroups.sort((g1, g2) -> Integer.compare(g2.size(), g1.size()));

        try (FileWriter writer = new FileWriter(filename)) {
            long groupCount = validGroups.size();
            System.out.println("Групп с более чем одной записью: " + groupCount);
            writer.write("Групп с более чем одной записью: " + groupCount);
            writer.write("\n-------------------------\n");

            int groupNumber = 1;
            for (Set<List<String>> group : validGroups) {
                writer.write("Группа " + groupNumber + ":\n");
                for (List<String> record : group) {
                    writer.write(String.join(";", record) + "\n");
                }
                writer.write("\n");
                groupNumber++;
            }
        }
    }
}