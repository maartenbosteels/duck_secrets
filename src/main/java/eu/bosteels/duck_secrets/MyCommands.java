package eu.bosteels.duck_secrets;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.util.List;
import java.util.Map;

@SuppressWarnings("SqlSourceToSinkFlow")
@ShellComponent
public class MyCommands {

  private JdbcClient jdbcClient;
  private static final Logger logger = LoggerFactory.getLogger(MyCommands.class);
  private static final String SELECT_VERSION = "select version() as duckdb_version";

  @PostConstruct
  public void init() {
    useDuckDataSource();
  }

  @ShellMethod(key = "duckDataSource")
  public void useDuckDataSource() {
    jdbcClient = JdbcClient.create(DuckDataSource.memory());
    logger.info("using a DuckDataSource");
    query(SELECT_VERSION);
  }

  @ShellMethod(key = "singleConnectionDataSource")
  public void useSingleConnectionDataSource() {
    SingleConnectionDataSource singleConnectionDataSource = new SingleConnectionDataSource(Reader.url_duckdb_memory, false);
    jdbcClient = JdbcClient.create(singleConnectionDataSource);
    logger.info("using a SingleConnectionDataSource");
  }

  @ShellMethod(key = "run")
  public void query(@ShellOption(defaultValue = SELECT_VERSION) String sql) {
    runQuery(sql);
  }

  private String duckdbVersion() {
    return jdbcClient.sql("select version()").query(String.class).single();
  }

  private void runQuery(String sql) {
    System.out.println("sql = '" + sql + "'");
    List<Map<String, Object>> rows = jdbcClient.sql(sql).query().listOfRows();
    System.out.println("Number of rows found: " + rows.size());
    int rowNr = 0;
    for (Map<String, Object> row : rows) {
      rowNr++;
      System.out.printf("Row %d : %s%n", rowNr, row);
    }
  }

  @ShellMethod(key = "show_secrets")
  public void show_secrets() {
    String sql = "select * from duckdb_secrets() where type = 's3'";
    System.out.println("sql = '" + sql + "'");
    List<Map<String, Object>> rows = jdbcClient.sql(sql).query().listOfRows();
    System.out.println("Number of secrets found: " + rows.size());
    int rowNr = 0;
    for (Map<String, Object> row : rows) {
      rowNr++;
      System.out.printf("Secret %d : %s%n", rowNr, row);
    }
  }

  @ShellMethod(key = "create_secret")
  public void create_secret(
          @ShellOption(defaultValue = "false") boolean persistent,
          @ShellOption(defaultValue = "false") boolean replace,
          @ShellOption(defaultValue = "false") boolean appendVersionToName,
          @ShellOption(defaultValue = "secret")  String name,
          @ShellOption(defaultValue = "s3") String type,
          @ShellOption(defaultValue = "credential_chain") String provider)
  {
    StringBuilder sql = new StringBuilder();
    if (appendVersionToName) {
      name = name + "_duckdb_" + duckdbVersion();
    }
    sql.append("CREATE ");
    if (replace) {
      sql.append(" OR REPLACE ");
    }
    if (persistent) {
      sql.append("PERSISTENT ");
    }
    sql.append("SECRET \"")
            .append(name)
            .append("\" (TYPE ").append(type)
            .append(", PROVIDER ").append(provider)
            .append(")");
    System.out.println("sql = '" + sql + "'");
    List<Map<String, Object>> rows = jdbcClient.sql(sql.toString()).query().listOfRows();
    System.out.println("Number of rows returned: " + rows.size());
    int rowNr = 0;
    for (Map<String, Object> row : rows) {
      rowNr++;
      System.out.printf("Secret %d : %s%n", rowNr, row);
    }
  }

  @ShellMethod(key = "read_from_s3")
  public void read_from_s3(
          @ShellOption(defaultValue = "bucket") String bucket,
          @ShellOption(defaultValue = "test/data.csv") String key,
          @ShellOption(defaultValue = "2") int limit)
  {
    StringBuilder sql = new StringBuilder();
    sql.append("select * from ")
            .append(" 's3://").append(bucket).append("/").append(key).append("'")
            .append(" limit ").append(limit);
    runQuery(sql.toString());
  }

  @ShellMethod(key = "execute")
  public void execute(@ShellOption(defaultValue = "drop secret if exists secret") String sql) {
    System.out.println("sql = '" + sql + "'");
    int rowsAffected = jdbcClient.sql(sql).update();
    System.out.println("Number of rows affected: " + rowsAffected);

  }

}
