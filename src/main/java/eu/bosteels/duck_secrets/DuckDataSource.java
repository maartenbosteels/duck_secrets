package eu.bosteels.duck_secrets;

import java.util.Properties;

import lombok.SneakyThrows;
import org.duckdb.DuckDBConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.AbstractDriverBasedDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

@SuppressWarnings("NullableProblems")
public class DuckDataSource extends AbstractDriverBasedDataSource implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DuckDataSource.class);

    private DuckDBConnection connection;

    @SneakyThrows
    public void init() {
        String url = getUrl();
        logger.info("Creating connection with url = {}", url);
        Objects.requireNonNull(url);
        this.connection = (DuckDBConnection) DriverManager.getConnection(url);
    }

    public DuckDataSource(String url) {
        setUrl(url);
        init();
    }

    public static DuckDataSource memory() {
        return new DuckDataSource("jdbc:duckdb:");
    }

    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.info("Could not close DuckDBConnection = {}", e.getMessage());
            }
        }
    }

    @Override
    protected Connection getConnectionFromDriver(Properties props) throws SQLException {
        logger.debug("getConnectionFromDriver with props = {}", props);
        return connection.duplicate();
    }


}
