package com.fantasy.kvdatabase.db;

import java.io.IOException;

/**
 * KV database 的基本操作  get/set
 */
public interface KvDB {
    String get(String key);

    void set(String key, String value);
}
