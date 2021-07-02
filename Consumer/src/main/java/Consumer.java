import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class Consumer {
  private final static String QUEUE_NAME = "threadExQ";
  private final static Integer THREADS = 10;

  public static void main(String[] argv) throws Exception {
    com.rabbitmq.client.ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("localhost");
    factory.setHost("ec2-100-25-150-0.compute-1.amazonaws.com");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("password");
    final com.rabbitmq.client.Connection connection = factory.newConnection();

    Runnable runnable = new Runnable() { //run for multiple threads
      @Override
      public void run() {
        try {
          final Channel channel = connection.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          // max one message per receiver
          channel.basicQos(1);
          System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            String[] tuple = message.split(": ");
            String SQL_QUERY = "INSERT INTO WordSchema.CountWords (word,wordCount) VALUES(\'" + tuple[0] +"\',\'" + Integer.valueOf(tuple[1]) + "\')";
            java.sql.Connection con = null;
            try {
              con = DataSource.getDataSource().getConnection();
              PreparedStatement pst = con.prepareStatement( SQL_QUERY );
              pst.executeUpdate();
            } catch (SQLException throwables) {
              throwables.printStackTrace();
            } finally {
              try {
                con.close();
              } catch (SQLException throwables) {
                throwables.printStackTrace();
              } ;
            }
          };
          channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    };

    //TODO: i would remove this once I have my channel pool in place right?
    for (int i=0; i < THREADS; i++) {
      Thread recv1 = new Thread(runnable);
      recv1.start();
    }
  }
}

class DataSource {
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
    // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html
    String url = String.format("jdbc:mysql://%s:%s/%s?serverTimezone=UTC&useSSL=false", HOST_NAME, PORT, DATABASE);
    ds.setJdbcUrl(url);
    ds.setUsername(USERNAME);
    ds.setPassword(PASSWORD);
  }

  public static HikariDataSource getDataSource() {
    return ds;
  }
}