package com.fantasy.kvdatabase.logfile;

import com.fantasy.kvdatabase.util.Constant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

/**
 * 追加写的日志文件，存储数据库数据，顺序写!
 */
public class LogFile {
    // 文件所在路径
    private Path dataPath;
    private Path idxPath;


    public LogFile(String path) {
        this.dataPath = Paths.get(path + Constant.DATA_FILE_SUFFIX);
        this.idxPath = Paths.get(path + Constant.IDX_FILE_SUFFIX);
    }

    public static LogFile create(String curPath) {
        return new LogFile(curPath);
    }

    /**
     * @param record 顺序写入数据
     * @return 返回写入数据的文件偏移位置
     */
    public long append(String record) {
        long offset = dataPath.toFile().exists() ? dataPath.toFile().length() : 0;

        try {
            record += System.lineSeparator();
            Files.write(dataPath, record.getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return offset;
    }

    /**
     * @param offset 文件偏移位置 offset
     * @return
     */
    public String read(long offset) {

        try (RandomAccessFile file = new RandomAccessFile(dataPath.toFile(), "r")) {
            file.seek(offset);
            return file.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }

    /**
     * 返回文件所有数据
     *
     * @return
     * @throws IOException
     */
    public Stream<String> lines() throws IOException {
        return Files.lines(dataPath);
    }

    /**
     * 返回文件大小
     *
     * @return
     */
    public long size() {
        return dataPath.toFile().length();
    }

    /**
     * 返回文件绝对路径名称
     *
     * @return
     */
    public String path() {
        return dataPath.toFile().getAbsolutePath().split("\\.")[0];
    }

    /**
     * 文件 rename
     *
     * @param s
     */
    public void renameTo(String s) {

        dataPath.toFile().renameTo(new File(s + Constant.DATA_FILE_SUFFIX));
        dataPath = Paths.get(s + Constant.DATA_FILE_SUFFIX);

        idxPath.toFile().renameTo(new File(s + Constant.IDX_FILE_SUFFIX));
        idxPath = Paths.get(s + Constant.IDX_FILE_SUFFIX);
    }

    /**
     * 删除数据文件
     */
    public void delete() throws IOException {
        if (dataPath.toFile().exists()) {
            Files.delete(dataPath);
        }
        if (idxPath.toFile().exists()) {
            Files.delete(idxPath);
        }
    }

    public Path getDataPath() {
        return dataPath;
    }

    public Path getIdxPath() {
        return idxPath;
    }

    @Override
    public String toString() {
        return "LogFile{" +
                "dataPath=" + dataPath +
                ", idxPath=" + idxPath +
                '}';
    }
}
