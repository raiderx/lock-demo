package org.karpukhin.lockdemo;

import java.sql.Connection;
import java.sql.SQLException;

interface TransactionTemplate<T> {
    T  doInTransaction(Connection connection) throws SQLException;
}
