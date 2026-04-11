package ru.ing.util;

import ru.ing.model.Key;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class StringSearch {

    public static final Pattern VALID_LINE_PATTERN = Pattern.compile("^(\"[^\"]*(\"{2}[^\"]*)*\";)*\"[^\"]*(\"{2}[^\"]*)*\"$");

    public void processData(String fileName) throws IOException {
        Map<Key, Integer> keyToGroupId = new HashMap<>();
        Map<Integer, List<List<String>>> largeGroups = new HashMap<>();
        List<GroupCandidate> pendingRecords = new ArrayList<>();

        int groupIdCounter = 0;

        try (SevenZFile sevenZFile = new SevenZFile(new File(fileName))) {
            SevenZArchiveEntry entry;
            while ((entry = sevenZFile.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".csv")) {
                    byte[] content = new byte[(int) entry.getSize()];
                    sevenZFile.read(content, 0, content.length);

                    try (BufferedReader br = new BufferedReader(
                            new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8))) {

                        String line;
                        while ((line = br.readLine()) != null) {
                            if (isValidLine(line)) {
                                List<String> record = parseRecord(line);

                                Set<Integer> matchingGroups = new HashSet<>();
                                for (int i = 0; i < record.size(); i++) {
                                    String value = record.get(i);
                                    if (!value.isEmpty()) {
                                        Key key = new Key(value, i);
                                        Integer groupId = keyToGroupId.get(key);
                                        if (groupId != null) {
                                            matchingGroups.add(groupId);
                                        }
                                    }
                                }

                                if (matchingGroups.isEmpty()) {
                                    int newGroupId = groupIdCounter++;
                                    for (int i = 0; i < record.size(); i++) {
                                        String value = record.get(i);
                                        if (!value.isEmpty()) {
                                            keyToGroupId.put(new Key(value, i), newGroupId);
                                        }
                                    }
                                    pendingRecords.add(new GroupCandidate(newGroupId, record));
                                } else {
                                    List<Integer> sortedGroups = new ArrayList<>(matchingGroups);
                                    sortedGroups.sort(Integer::compareTo);
                                    int mainGroupId = sortedGroups.get(0);

                                    for (int idx = 1; idx < sortedGroups.size(); idx++) {
                                        int groupIdToMerge = sortedGroups.get(idx);

                                        List<List<String>> recordsToMerge = largeGroups.get(groupIdToMerge);
                                        if (recordsToMerge != null) {
                                            largeGroups.computeIfAbsent(mainGroupId, k -> new ArrayList<>())
                                                    .addAll(recordsToMerge);
                                            largeGroups.remove(groupIdToMerge);
                                        }

                                        Iterator<GroupCandidate> iterator = pendingRecords.iterator();
                                        while (iterator.hasNext()) {
                                            GroupCandidate candidate = iterator.next();
                                            if (candidate.groupId == groupIdToMerge) {
                                                iterator.remove();
                                                largeGroups.computeIfAbsent(mainGroupId, k -> new ArrayList<>())
                                                        .add(candidate.record);
                                            }
                                        }

                                        keyToGroupId.entrySet().removeIf(e -> e.getValue().equals(groupIdToMerge));
                                    }

                                    for (int i = 0; i < record.size(); i++) {
                                        String value = record.get(i);
                                        if (!value.isEmpty()) {
                                            keyToGroupId.put(new Key(value, i), mainGroupId);
                                        }
                                    }

                                    List<List<String>> mainGroupRecords = largeGroups.get(mainGroupId);
                                    if (mainGroupRecords != null) {
                                        mainGroupRecords.add(record);
                                    } else {
                                        boolean found = false;
                                        for (GroupCandidate candidate : pendingRecords) {
                                            if (candidate.groupId == mainGroupId) {
                                                pendingRecords.remove(candidate);
                                                List<List<String>> newGroup = new ArrayList<>();
                                                newGroup.add(candidate.record);
                                                newGroup.add(record);
                                                largeGroups.put(mainGroupId, newGroup);
                                                found = true;
                                                break;
                                            }
                                        }
                                        if (!found) {
                                            pendingRecords.add(new GroupCandidate(mainGroupId, record));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        Map<Integer, List<List<String>>> finalGroups = new HashMap<>();

        for (GroupCandidate candidate : pendingRecords) {
            int size = getGroupSize(candidate.groupId, pendingRecords, largeGroups);
            if (size > 1) {
                finalGroups.computeIfAbsent(candidate.groupId, k -> new ArrayList<>())
                        .add(candidate.record);
            }
        }

        for (Map.Entry<Integer, List<List<String>>> entry : largeGroups.entrySet()) {
            if (entry.getValue().size() > 1) {
                finalGroups.put(entry.getKey(), entry.getValue());
            }
        }

        writeResultsToFile(finalGroups);
    }

    protected List<String> parseRecord(String line) {
        String[] values = line.split(";");
        List<String> result = new ArrayList<>(values.length);
        for (String value : values) {
            if (value.startsWith("\"") && value.endsWith("\"")) {
                result.add(value.substring(1, value.length() - 1));
            } else {
                result.add(value);
            }
        }
        return result;
    }

    private int getGroupSize(int groupId, List<GroupCandidate> pending,
                             Map<Integer, List<List<String>>> largeGroups) {
        int size = 0;
        for (GroupCandidate candidate : pending) {
            if (candidate.groupId == groupId) size++;
        }
        List<List<String>> largeGroup = largeGroups.get(groupId);
        if (largeGroup != null) {
            size += largeGroup.size();
        }
        return size;
    }

    protected static boolean isValidLine(String line) {
        return VALID_LINE_PATTERN.matcher(line).matches();
    }

    protected void writeResultsToFile(Map<Integer, List<List<String>>> groups) throws IOException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmm");
        String filename = "output_" + dateFormat.format(new Date()) + ".txt";

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

    private static class GroupCandidate {
        int groupId;
        List<String> record;

        GroupCandidate(int groupId, List<String> record) {
            this.groupId = groupId;
            this.record = record;
        }
    }
}