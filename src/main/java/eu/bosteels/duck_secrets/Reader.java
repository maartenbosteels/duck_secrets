package eu.bosteels.duck_secrets;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;

@SuppressWarnings("SqlNoDataSourceInspection")
@Component
public class Reader {

//    @Value("${s3_location}")
//    private String s3_location;

    public final static String url_duckdb_memory = "jdbc:duckdb:";
//    private String SELECT;
//    private final static String CREATE_SECRET = "CREATE OR REPLACE SECRET secret (TYPE s3,  PROVIDER credential_chain)";
//    private final static String CREATE_PERSISTENT_SECRET = "CREATE OR REPLACE persistent SECRET secret (TYPE s3,  PROVIDER credential_chain)";

    private static final Logger logger = LoggerFactory.getLogger(Reader.class);

//    @PostConstruct
//    public void init() {
//
//        logger.info("s3_location = {}", s3_location);
//        SELECT = "SELECT * FROM " + s3_location + " limit 2";
//
//        //testAllCombinations();
//    }


    public enum SecretType {
        NONE, NORMAL, PERSISTENT
    }

    public void logVersion() {
        JdbcClient jdbcClient = JdbcClient.create(DuckDataSource.memory());
        String version = jdbcClient.sql("select version()").query(String.class).single();
        logger.info("DuckDB version = {}", version);
    }

//    public void testAllCombinations() {
//
//        logVersion();
//
//        // SecretType.values()
//        for (SecretType secretType : List.of(SecretType.NONE, SecretType.NORMAL)) {
//            logger.info("==== secretType = {} =====", secretType);
//            {
//                boolean ok = withRawJdbc(secretType);
//                logger.info("Access: RAW JDBC | Secret: {} OK: {}", secretType, ok);
//            }
//            {
//                boolean ok = withSingleConnectionDataSource(secretType);
//                logger.info("Access: SingleConnection | Secret: {} OK: {}", secretType, ok);
//            }
//            {
//                boolean ok = withDuckDataSource(secretType);
//                logger.info("Access: DuckDataSource | Secret: {} OK: {}", secretType, ok);
//            }
//            logger.info("==================", secretType);
//        }
//    }

    private void dropSecret(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            stmt.execute("drop secret if exists secret");
            logSecrets(conn);
        } catch (Exception e) {
            logger.warn(e.getMessage());
        }
    }

    public void logSecrets(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            try (ResultSet rs = stmt.executeQuery("select * from duckdb_secrets()")) {
                while (rs.next()) {
                    String columnName = rs.getMetaData().getColumnClassName(1);
                    String value = rs.getString(1);
                    logger.info("columnName={} value='{}'", columnName, value);
                }
            }
            stmt.close();

        } catch (Exception e) {
            logger.error("Read secrets: {}", e.getMessage());
        }

    }


//    private boolean withRawJdbc(SecretType secretType) {
//        try {
//            Connection conn = DriverManager.getConnection(url_duckdb_memory);
//            dropSecret(conn);
//            Statement stmt = conn.createStatement();
//
//            if (secretType == SecretType.NORMAL) {
//                stmt.execute(CREATE_SECRET);
//            }
//            if (secretType == SecretType.PERSISTENT) {
//                stmt.execute(CREATE_PERSISTENT_SECRET);
//            }
//
//            try (ResultSet rs = stmt.executeQuery(SELECT)) {
//                while (rs.next()) {
//                    String columnName = rs.getMetaData().getColumnClassName(1);
//                    String value = rs.getString(1);
//                    //System.out.printf("%s => '%s' %n", columnName, value);
//                    logger.info("columnName={} value='{}'", columnName, value);
//                }
//            }
//            stmt.close();
//            return true;
//
//        } catch (Exception e) {
//            logger.error("Raw JDBC, secret:{} error:{}", secretType, e.getMessage());
//            return false;
//        }
//    }

    public void dropSecret(JdbcClient jdbcClient, String name) {
        try {
            String stmt = "drop secret " + name;
            jdbcClient.sql(stmt).update();
            logger.info("stmt: {}", stmt);
        } catch (Exception e) {
            logger.warn("drop secret failed: {}", e.getMessage());
        }
    }

//    public void createSecret(JdbcClient jdbcClient, SecretType secretType) {
//        String stmt = "";
//        if (secretType == SecretType.NONE) {
//            return;
//        }
//        if (secretType == SecretType.NORMAL) {
//            stmt = CREATE_SECRET;
//        }
//        if (secretType == SecretType.PERSISTENT) {
//            stmt = CREATE_PERSISTENT_SECRET;
//        }
//        logger.info("stmt: {}", stmt);
//        Object secretCreated = jdbcClient.sql(stmt).query().singleColumn();
//        logger.info("secretCreated = {}", secretCreated);
//
//    }

//    public boolean withSingleConnectionDataSource(SecretType secretType) {
//        logger.info("==== withSingleConnectionDataSource ====");
//        SingleConnectionDataSource ds = new SingleConnectionDataSource(url_duckdb_memory, false);
//        return readRows(secretType, ds);
//    }
//
//    public boolean withDuckDataSource(SecretType secretType) {
//        logger.info("==== withDuckDataSource ====");
//        DataSource dataSource = DuckDataSource.memory();
//        return readRows(secretType, dataSource);
//    }

    private void logSecrets(JdbcClient jdbcClient) {
        List<Map<String, Object>> rows = jdbcClient.sql("select * from duckdb_secrets()").query().listOfRows();
        logger.info("we found {} secrets", rows.size());
        for (Map<String, Object> row : rows) {
            logger.info("secret: {}", row);
        }
    }

//    private boolean readRows(SecretType secretType, DataSource dataSource) {
//        try {
//            JdbcClient jdbcClient = JdbcClient.create(dataSource);
//
//            JdbcClient jdbcClient_secret = JdbcClient.create(dataSource);
//
//            dropSecret(jdbcClient, "secret");
//
//            //logSecrets(jdbcClient);
//
//            createSecret(jdbcClient_secret, secretType);
//
//
//            List<Map<String, Object>> rows = jdbcClient.sql(SELECT).query().listOfRows();
//            logger.info("rows = {}", rows.size());
//            for (Map<String, Object> row : rows) {
//                logger.info("row = {}", row);
//            }
//            return true;
//        } catch (Exception e) {
//            logger.error("readRows: secretType: {} => {}", secretType, e.getMessage());
//            //logger.error("failed", e);
//            return false;
//        }
//    }


}
