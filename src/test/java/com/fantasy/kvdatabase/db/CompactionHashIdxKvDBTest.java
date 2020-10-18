package com.fantasy.kvdatabase.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CompactionHashIdxKvDBTest {
    private CompactionHashIdxKvDB compactionHashIdxKvDB;

    @Before
    public void createBD() {
        compactionHashIdxKvDB = new CompactionHashIdxKvDB();
    }

    @Test
    public void testGet() {
        for (int i = 30; i < 40; ++i) {
            compactionHashIdxKvDB.set(String.valueOf(i), String.valueOf(i + 1));
        }
    }

    @Test
    public void testSet() throws InterruptedException {
        System.out.println(compactionHashIdxKvDB.get(String.valueOf(1)));
        System.out.println(compactionHashIdxKvDB.get(String.valueOf(8)));
        System.out.println(compactionHashIdxKvDB.get(String.valueOf(20)));


        // Thread.sleep(120 * 1000);
    }

    @After
    public void closeDB() {
        compactionHashIdxKvDB.close();
    }
}
