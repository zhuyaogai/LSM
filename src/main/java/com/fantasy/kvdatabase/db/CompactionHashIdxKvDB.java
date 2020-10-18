package com.fantasy.kvdatabase.db;

import com.fantasy.kvdatabase.index.MultiHashIdx;
import com.fantasy.kvdatabase.logfile.LogFile;
import com.fantasy.kvdatabase.util.Constant;
import com.fantasy.kvdatabase.util.Util;
import javafx.util.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 *  
 *  追加写，当当前segment file到达一定大小后，追加到新到segment file上。并定时对旧的segment file进行compact。 
 *  为每个segment file维持一个哈希索引，提升读性能
 *  支持单线程写，多线程读 (这个如何实现?)
 */
public class CompactionHashIdxKvDB implements KvDB {

    private final static long MAX_LOG_SIZE_BYTE = 24;

    // 当前追加的segment file路径
    private LogFile curLog;

    // 写old segment file集合，会定时对这些文件进行level1 compact合并
    private Deque<LogFile> toCompact;

    // level1 compacted segment file集合，会定时对这些文件进行level2 compact合并
    private Deque<LogFile> compactedLevel1;

    // level2 compacted segment file集合
    private Deque<LogFile> compactedLevel2;

    // 多segment file哈希索引
    private MultiHashIdx idx;

    // 对compact的定时调度
    private ScheduledExecutorService compactExecutor;

    private AtomicInteger toCompactNum;

    private AtomicInteger level1Num;

    private AtomicInteger level2Num;


    public CompactionHashIdxKvDB() {
        this.toCompact = new LinkedList<>();
        this.compactedLevel1 = new LinkedList<>();
        this.compactedLevel2 = new LinkedList<>();
        this.idx = new MultiHashIdx();
        // this.compactExecutor = Executors.newFixedThreadPool(10);
        this.toCompactNum = new AtomicInteger(0);
        this.level1Num = new AtomicInteger(0);
        this.level2Num = new AtomicInteger(0);

        loadIdx();

        compactExecutor = Executors.newScheduledThreadPool(2);
        // 合并 compactLevel1
        compactExecutor.scheduleAtFixedRate(() -> {
            try {
                compactLevel1();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 30, 30, TimeUnit.SECONDS);

        // 合并 compactLevel2
        compactExecutor.scheduleAtFixedRate(() -> {
            try {
                compactLevel2();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    /**
     * 当前追加的LogFile->toCompact队列->compactedLevel1队列->compactedLevel2队列
     *
     * @param key
     * @return
     */
    @Override
    public String get(String key) {

        // 第一步：从当前的LogFile查找
        if (idx.idxOf(curLog, key) != -1) {
            long offset = idx.idxOf(curLog, key);

            System.out.println(String.format("在当前logfile找到 %s", key));
            return Util.valueOf(curLog.read(offset));
        }

        // 第二步：从toCompact中查找
        String value = find(key, toCompact);
        if (!value.isEmpty()) {
            System.out.println(String.format("在toCompact中找到 %s", key));
            return value;
        }

        // 第三步：从 compactedLevel1 中查找
        value = find(key, compactedLevel1);
        if (!value.isEmpty()) {
            System.out.println(String.format("在compactedLevel1中找到 %s", key));
            return value;
        }

        // 第四步：从 compactedLevel2 中查找
        value = find(key, compactedLevel2);
        if (!value.isEmpty()) {
            System.out.println(String.format("在compactedLevel2中找到 %s", key));
            return value;
        }

        System.out.println(String.format("找不到 %s", key));
        return "";
    }

    /**
     * 从队列查找key
     *
     * @param key          查找key
     * @param logFileDeque LogFile queues
     * @return
     */
    private String find(String key, Deque<LogFile> logFileDeque) {

        final List<LogFile> collect =
                logFileDeque.stream().filter(x -> idx.idxOf(x, key) != -1).collect(Collectors.toList());

        if (collect.size() > 0) {
            LogFile logFile = collect.get(collect.size() - 1);  // 寻找最新的那一个
            return Util.valueOf(logFile.read(idx.idxOf(logFile, key)));
        } else {
            return "";
        }
    }

    @Override
    public void set(String key, String value) {

        // 如果当前LogFile写满了，则放到toCompact队列中，并创建新的LogFile
        if (curLog.size() >= MAX_LOG_SIZE_BYTE) {
            String curPath = curLog.path();  // 可以新增写的文件
            Map<String, Long> oldIdx = idx.allIdxOf(curLog);
            curLog.renameTo(curPath + "_" + toCompactNum.getAndIncrement());
            toCompact.addLast(curLog);
            // 创建新的文件后，索引也要更新
            idx.addAllIdx(curLog, oldIdx);
            curLog = LogFile.create(curPath);
            idx.cleanIdx(curLog);
        }

        final String record = Util.composeRecord(key, value);
        idx.addIdx(curLog, key, curLog.append(record));
    }


    /**
     * 进行level1 compact，对单个old segment file合并
     */
    private void compactLevel1() throws IOException {
        System.out.println("进行Level1 Compact!!!");
        while (!toCompact.isEmpty()) {
            // 创建新的level1 compacted segment file
            // level1的文件命名规则为：filename_level1_num
            LogFile newLogFile = LogFile.create(curLog.path() + "_level1_" + level1Num.getAndIncrement());
            LogFile logFile = toCompact.getFirst();

            /*
             这种情况之下，只可以覆盖同一个logfile内的重复记录，不同的之间依然是可以出现重复的
             */
            idx.allIdxOf(logFile).forEach((key, offset) -> {
                String record = logFile.read(offset);

                final long offset1 = newLogFile.append(record);
                idx.addIdx(newLogFile, key, offset1);
            });

            // 写完后存储到compactedLevel1队列中，并删除toCompact中对应的文件
            compactedLevel1.addLast(newLogFile);
            toCompact.pollFirst();
            deleteLogFile(logFile);
        }
    }


    /**
     * 进行level2 compact，针对compactedLevel1队列中所有的文件进行合并
     */
    private void compactLevel2() throws IOException {
        System.out.println("进行Level2 Compact!!!");
        // 生成一份快照
        Deque<LogFile> snapshot = new LinkedList<>(compactedLevel1);
        if (snapshot.isEmpty()) {
            return;
        }

        int compactSize = snapshot.size();
        // level2的文件命名规则为：filename_level2_num
        LogFile newLogFile = LogFile.create(curLog.path() + "_level2_" + level2Num.getAndIncrement());
        while (!snapshot.isEmpty()) {
            // 从最新的level1 compacted segment file开始处理
            LogFile logFile = snapshot.pollLast();
            idx.allIdxOf(logFile).forEach((key, offset) -> {
                if (idx.idxOf(newLogFile, key) == -1) {
                    final String record = logFile.read(offset);
                    idx.addIdx(newLogFile, key, newLogFile.append(record));
                }
            });
        }

        compactedLevel2.addLast(newLogFile);
        // 写入完成后，删除compactedLevel1队列中相应的文件
        while (compactSize > 0) {
            LogFile logFile = compactedLevel1.pollFirst();
            deleteLogFile(logFile);
            --compactSize;
        }
    }

    private void deleteLogFile(LogFile logFile) throws IOException {
        idx.cleanIdx(logFile);
        logFile.delete();
    }


    /**
     * TODO: 1.加载所有index数据进来初始化 2.初始化curLog
     */
    private void loadIdx() {
        // 获取目录下所有文件
        final File[] files = new File(Constant.DATA_FILE_PREFIX).listFiles();
        if (files != null && files.length > 0) {
            // 先排序
            Arrays.sort(files, Comparator.comparing(File::getName));

            Arrays.stream(files).forEach(file -> {
                try {
                    if (file.getAbsolutePath().endsWith(".data")) {
                        final String fileName = Util.getFileName(file.getAbsolutePath());
                        LogFile logFile = new LogFile(fileName);

                        if (new File(fileName + Constant.IDX_FILE_SUFFIX).exists()) {
                            Files.lines(Paths.get(fileName + Constant.IDX_FILE_SUFFIX)).forEach(x -> {
                                final Pair<String, Long> pair = Util.decomposeIdx(x);
                                idx.addIdx(logFile, pair.getKey(), pair.getValue());
                            });
                        }

                        if (fileName.contains("_level1_")) {
                            compactedLevel1.addLast(logFile);
                            level1Num = new AtomicInteger(
                                    Math.max(level1Num.get(),
                                            Integer.parseInt(fileName.substring(fileName.indexOf("_level1_") + "_level1_".length()))));
                        } else if (fileName.contains("_level2_")) {
                            compactedLevel2.addLast(logFile);
                            level2Num = new AtomicInteger(
                                    Math.max(level2Num.get(),
                                            Integer.parseInt(fileName.substring(fileName.indexOf("_level2_") + "_level2_".length()))));
                        } else if (fileName.contains("_")) {
                            toCompact.addLast(logFile);
                            toCompactNum = new AtomicInteger(
                                    Math.max(toCompactNum.get(),
                                            Integer.parseInt(fileName.substring(fileName.indexOf("_") + "_".length()))));
                        } else {
                            curLog = logFile;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        if (curLog == null) {
            curLog = new LogFile(Constant.DATA_FILE_URL);
        }
    }

    /**
     * TODO: dump所有idx文件，即 idx 中的所有内容
     */
    private void dumpIdx() {
        idx.dumpIdx();
    }


    public void close() {
        dumpIdx();
    }

    /**
     *  TODO: 开启线程调度
     */
}
