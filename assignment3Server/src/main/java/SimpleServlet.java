import com.google.gson.Gson;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "SimpleServlet", value = "/SimpleServlet")
public class SimpleServlet extends HttpServlet {
  protected Gson gson = new Gson();
  private final static String QUEUE_NAME = "threadExQ";
  private GenericObjectPool<Channel> factory;
  private final static Integer getThreads = 10;



  public SimpleServlet() {
    //create connection
    factory = new GenericObjectPool<>(new ChannelFactory());
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    BufferedReader buff = request.getReader();
    textMessage messageObj = new Gson().fromJson(buff.readLine(), textMessage.class);
    String message = messageObj.getMessage();

    String SQL_QUERY = "SELECT * FROM WordSchema.CountWords word = \'" + message + "\'";
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
      }
      ;
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {

    BufferedReader buff = request.getReader();
    textMessage messageObj = new Gson().fromJson(buff.readLine(), textMessage.class);
    String message = messageObj.getMessage();

    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    String function = request.getPathInfo();
    PrintWriter out = response.getWriter();

    if (message == null || function == null) {
      BigDecimal jsonObject = new BigDecimal(-1);
      String returnStmt = this.gson.toJson(jsonObject);
      response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
      response.getWriter().write(returnStmt);
      out.print(returnStmt);
    } else {
      BigDecimal jsonObject = new BigDecimal(0);
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().write(new Gson().toJson(jsonObject));
    }

    String[] wordList = message.split(" ");
    HashMap<String, Integer> tuples = new HashMap<>();

    for (String word: wordList) {
      if (!word.isEmpty()) {
        if (!tuples.containsKey(word)) {
          tuples.put(word, 1);
        } else {
          tuples.put(word, tuples.get(word) + 1);
        }
      }
    }

    try {
      Channel channel = factory.borrowObject();

      channel.queueDeclare(QUEUE_NAME, true, false, false, null);

      for (String word : tuples.keySet()) {
        String mes = word + ": " + tuples.get(word);
        channel.basicPublish("", QUEUE_NAME, null, mes.getBytes(StandardCharsets.UTF_8));
      }
      factory.returnObject(channel);
    } catch (Exception e) {
      System.out.print(e);
    }
  }
}