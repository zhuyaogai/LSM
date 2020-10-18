package com.fantasy.kvdatabase.db;

import com.fantasy.kvdatabase.logfile.LogFile;
import com.fantasy.kvdatabase.util.Util;
import javafx.util.Pair;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 需要保存索引
 */
public class HashIdxKvDB implements KvDB {
    private final LogFile logFile;     // 存储数据
    private final Map<String, Long> idx;     // 存储索引
    private final Path idxPath;

    public HashIdxKvDB(String path, String idxPath) throws IOException {
        this.logFile = new LogFile(path);
        this.idx = new ConcurrentHashMap<>();
        this.idxPath = Paths.get(idxPath);

        loadIdx();   // load index
    }

    /**
     * 获取索引文件
     *
     * @return
     */
    public Map<String, Long> getIdx() {
        return idx;
    }


    @Override
    public String get(String key) {
        if (!idx.containsKey(key)) {
            return "";
        }

        return Util.valueOf(logFile.read(idx.get(key)));
    }

    @Override
    public void set(String key, String value) {
        final long offset = logFile.append(Util.composeRecord(key, value));
        idx.put(key, offset);
    }

    /**
     * close database and dump index
     *
     * @throws IOException
     */
    public void close() throws IOException {
        dumpIdx();
    }

    /**
     * dump index
     *
     * @throws IOException
     */
    private void dumpIdx() throws IOException {
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
    }

    /**
     * load index
     *
     * @throws IOException
     */
    private void loadIdx() throws IOException {
        if (idxPath.toFile().exists()) {
            Files.lines(idxPath).forEach(x -> {
                final Pair<String, Long> pair = Util.decomposeIdx(x);
                idx.put(pair.getKey(), pair.getValue());
            });
        }
    }
}
