package io.litecache;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Functional interface for SQL operations that may throw SQLException.
 */
@FunctionalInterface
interface SqlFunction<T> {
    /**
     * Applies the function to the given connection.
     *
     * @param conn the database connection
     * @return the result of the operation
     * @throws SQLException if a database error occurs
     */
    T apply(Connection conn) throws SQLException;
}
