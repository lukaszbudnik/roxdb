package com.github.lukaszbudnik.roxdb.rocksdb;

import org.rocksdb.RocksDBException;

@FunctionalInterface
public interface TransactionOperations {
    void doInTransaction(TransactionContext transactionContext) throws RocksDBException;
}
