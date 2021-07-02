import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSource {
  private static HikariConfig config = new HikariConfig();
  private static HikariDataSource ds;
  private static String HOST_NAME = "lab1.crlmsdnarnx1.us-east-1.rds.amazonaws.com";
  private static String PORT = "3306";
  private static String DATABASE = "lab1";
  private static String USERNAME = "lab1";
  private static String PASSWORD = "password";

  static {
    ds = new HikariDataSource();
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    String url = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&useSSL=false", HOST_NAME, PORT, DATABASE);
    ds.setJdbcUrl(url);
    ds.setUsername(USERNAME);
    ds.setPassword(PASSWORD);
  }

  public static HikariDataSource getDataSource() {
    return ds;
  }
}
