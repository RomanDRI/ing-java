package ru.ing.util;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class StringSearch {

    public void processData(String fileName) throws IOException {
        Map<String, Integer> valueToGroupId = new HashMap<>();
        List<Set<String>> groups = new ArrayList<>();
        Map<Integer, Set<Integer>> groupConnections = new HashMap<>();

        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(new FileInputStream(fileName), StandardCharsets.UTF_8))) {

            String line;
            while ((line = br.readLine()) != null) {

                if (isValidLine(line)) {
                    List<String> record = parseRecord(line);
                    processRecord(record, valueToGroupId, groups, groupConnections);
                }
            }
        }

        List<Set<String>> mergedGroups = mergeConnectedGroups(groups, groupConnections);
        writeResultsToFile(mergedGroups);
    }

    private void processRecord(List<String> record,
                               Map<String, Integer> valueToGroupId,
                               List<Set<String>> groups,
                               Map<Integer, Set<Integer>> groupConnections) {

        String recordStr = String.join(";", record);
        Set<Integer> matchingGroupIds = new HashSet<>();

        for (int colIndex = 0; colIndex < record.size(); colIndex++) {
            String value = record.get(colIndex);
            if (!value.isEmpty()) {
                String key = colIndex + ":" + value;
                Integer groupId = valueToGroupId.get(key);
                if (groupId != null) {
                    matchingGroupIds.add(groupId);
                }
            }
        }

        if (matchingGroupIds.isEmpty()) {
            Set<String> newGroup = new HashSet<>();
            newGroup.add(recordStr);
            groups.add(newGroup);
            int newGroupId = groups.size() - 1;

            for (int colIndex = 0; colIndex < record.size(); colIndex++) {
                String value = record.get(colIndex);
                if (!value.isEmpty()) {
                    String key = colIndex + ":" + value;
                    valueToGroupId.put(key, newGroupId);
                }
            }
        } else if (matchingGroupIds.size() == 1) {
            int groupId = matchingGroupIds.iterator().next();
            groups.get(groupId).add(recordStr);

            for (int colIndex = 0; colIndex < record.size(); colIndex++) {
                String value = record.get(colIndex);
                if (!value.isEmpty()) {
                    String key = colIndex + ":" + value;
                    valueToGroupId.put(key, groupId);
                }
            }
        } else {
            List<Integer> sortedGroupIds = new ArrayList<>(matchingGroupIds);
            int mainGroupId = sortedGroupIds.get(0);

            groups.get(mainGroupId).add(recordStr);

            for (int i = 1; i < sortedGroupIds.size(); i++) {
                int otherGroupId = sortedGroupIds.get(i);
                groupConnections.computeIfAbsent(mainGroupId, k -> new HashSet<>())
                        .add(otherGroupId);
                groupConnections.computeIfAbsent(otherGroupId, k -> new HashSet<>())
                        .add(mainGroupId);
            }

            for (int colIndex = 0; colIndex < record.size(); colIndex++) {
                String value = record.get(colIndex);
                if (!value.isEmpty()) {
                    String key = colIndex + ":" + value;
                    valueToGroupId.put(key, mainGroupId);
                }
            }
        }
    }

    private List<Set<String>> mergeConnectedGroups(List<Set<String>> groups,
                                                   Map<Integer, Set<Integer>> connections) {
        boolean[] visited = new boolean[groups.size()];
        List<Set<String>> result = new ArrayList<>();
        Map<Integer, Integer> oldToNewGroupId = new HashMap<>();

        for (int i = 0; i < groups.size(); i++) {
            if (!visited[i] && groups.get(i) != null) {
                Set<String> mergedGroup = new HashSet<>();
                Queue<Integer> queue = new LinkedList<>();
                queue.add(i);
                visited[i] = true;

                while (!queue.isEmpty()) {
                    int currentId = queue.poll();
                    mergedGroup.addAll(groups.get(currentId));
                    oldToNewGroupId.put(currentId, result.size());

                    Set<Integer> connected = connections.get(currentId);
                    if (connected != null) {
                        for (int connectedId : connected) {
                            if (!visited[connectedId]) {
                                visited[connectedId] = true;
                                queue.add(connectedId);
                            }
                        }
                    }
                }

                result.add(mergedGroup);
            }
        }

        return result;
    }

    protected List<String> parseRecord(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ';' && !inQuotes) {
                result.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        result.add(current.toString());

        return result;
    }

    protected static boolean isValidLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return false;
        }

        int quoteCount = 0;
        for (char c : line.toCharArray()) {
            if (c == '"') {
                quoteCount++;
            }
        }

        return quoteCount % 2 == 0 && line.contains("\"");
    }

    protected void writeResultsToFile(List<Set<String>> groups) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
        String filename = "output_" + dateFormat.format(new Date()) + ".txt";

        List<Set<String>> validGroups = new ArrayList<>();
        for (Set<String> group : groups) {
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
            for (Set<String> group : validGroups) {
                writer.write("Группа " + groupNumber + ":\n");
                for (String record : group) {
                    writer.write(record + "\n");
                }
                writer.write("\n");
                groupNumber++;
            }
        }
    }
}