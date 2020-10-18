package com.fantasy.kvdatabase.index;

import com.fantasy.kvdatabase.logfile.LogFile;
import com.fantasy.kvdatabase.util.Util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 给HashIdxKvDB加上compact机制
 */
public class MultiHashIdx {
    // 保存每一个 LogFile 对应的 Index Table
    private Map<LogFile, Map<String, Long>> idxs;

    public MultiHashIdx() {
        this.idxs = new ConcurrentHashMap<>();
    }

    public long idxOf(LogFile logFile, String key) {
        if (idxs.containsKey(logFile) && idxs.get(logFile).containsKey(key)) {
            return idxs.get(logFile).get(key);
        }

        return -1;  // -1 代表没有查找到该key
    }

    public void addIdx(LogFile logFile, String key, long offset) {
        idxs.putIfAbsent(logFile, new ConcurrentHashMap<>());
        idxs.get(logFile).put(key, offset);
    }

    public Map<String, Long> allIdxOf(LogFile curLog) {
        return idxs.get(curLog);
    }

    public void addAllIdx(LogFile curLog, Map<String, Long> oldIdx) {
        idxs.put(curLog, oldIdx);
    }

    /**
     * 清楚索引
     *
     * @param curLog
     */
    public void cleanIdx(LogFile curLog) {
        idxs.remove(curLog);
    }

    /**
     * dump Idx
     */
    public void dumpIdx() {
        idxs.forEach((logFile, idx) -> {
            try {
                final Path idxPath = logFile.getIdxPath();
                if (idxPath.toFile().exists()) {
                    Files.delete(idxPath);
                }

                idx.forEach((k, v) -> {
                    try {
                        Files.write(idxPath, Util.buildIdxRecord(k, v).getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public Map<LogFile, Map<String, Long>> getIdxs() {
        return idxs;
    }
}
