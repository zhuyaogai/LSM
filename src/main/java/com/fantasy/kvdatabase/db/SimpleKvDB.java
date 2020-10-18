package com.fantasy.kvdatabase.db;

import com.fantasy.kvdatabase.logfile.LogFile;
import com.fantasy.kvdatabase.util.Util;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 最简单的kv数据库：
 * 把数据按照 KV pair 存进文本文件，只是 append only，查找的时候按照key取最新的那一条数据
 */
public class SimpleKvDB implements KvDB {

    private LogFile logFile;

    public SimpleKvDB(String path) {
        this.logFile = new LogFile(path);
    }

    public String get(String key) {
        try (Stream<String> lines = logFile.lines()) {
            // 寻找最后匹配的那一条数据
            final List<String> values = lines.filter(x -> Util.matchKey(key, x))
                    .map(Util::valueOf)
                    .collect(Collectors.toList());

            return values.size() == 0 ? "" : values.get(values.size() - 1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }

    public void set(String key, String value) {
        // 简单追加数据到文件末尾
        this.logFile.append(Util.composeRecord(key, value));
    }
}
