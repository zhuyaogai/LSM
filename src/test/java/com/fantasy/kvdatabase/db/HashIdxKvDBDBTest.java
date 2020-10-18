package com.fantasy.kvdatabase.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HashIdxKvDBDBTest {
    private HashIdxKvDB hashIdxKvDB;

    @Before
    public void createBD() throws IOException {
        String path = "D:\\IdeaProjects\\KVDatabase\\src\\database.log";
        String idxPath = "D:\\IdeaProjects\\KVDatabase\\src\\database.idx";
        hashIdxKvDB = new HashIdxKvDB(path, idxPath);
    }

    @Test
    public void testGet() {
        for (int i = 0; i < 10; ++i) {
            hashIdxKvDB.set(String.valueOf(i), String.valueOf(i + 1));
        }

    }

    @Test
    public void testSet() throws InterruptedException {
        System.out.println(hashIdxKvDB.get(String.valueOf(1)));
        System.out.println(hashIdxKvDB.get(String.valueOf(8)));
        System.out.println(hashIdxKvDB.get(String.valueOf(20)));
    }

    @After
    public void closeDB() throws IOException {
        hashIdxKvDB.close();
    }
}
