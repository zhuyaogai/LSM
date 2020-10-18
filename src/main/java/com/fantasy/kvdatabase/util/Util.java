package com.fantasy.kvdatabase.util;

import javafx.util.Pair;

public class Util {
    private final static String DELIMITER = ",";

    public static String composeRecord(String key, String value) {
        return String.format("%s%s%s", key, DELIMITER, value);
    }

    public static Pair<String, Long> decomposeIdx(String record) {
        return new Pair<>(keyOf(record), Long.parseLong(valueOf(record)));
    }

    public static String buildIdxRecord(String key, Long offset) {
        return String.format("%s%s%s%s", key, DELIMITER, offset, System.lineSeparator());
    }

    public static boolean matchKey(String key, String line) {
        return keyOf(line).equals(key);
    }

    public static String keyOf(String record) {
        return record.split(DELIMITER)[0];
    }

    public static String valueOf(String record) {
        return record.split(DELIMITER)[1];
    }

    public static String getFileName(String fullName) {
        return fullName.split("\\.")[0];
    }
}
