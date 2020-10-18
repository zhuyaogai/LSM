package com.fantasy.kvdatabase.db;

import org.junit.Before;
import org.junit.Test;

public class SimpleKvDBTest {
    private SimpleKvDB simpleKvDB;

    @Before
    public void createBD() {
        String path = "D:\\IdeaProjects\\KVDatabase\\src\\database.data";
        simpleKvDB = new SimpleKvDB(path);
    }

    @Test
    public void testGet() {
        for (int i = 0; i < 10; ++i) {
            simpleKvDB.set(String.valueOf(i), String.valueOf(i + 1));
        }
    }

    @Test
    public void testSet() {
        System.out.println(simpleKvDB.get(String.valueOf(1)));
        System.out.println(simpleKvDB.get(String.valueOf(20)));
    }
}
