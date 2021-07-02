import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;

public class ChannelFactory
        extends BasePooledObjectFactory<Channel> {
  private Connection conn;

  ChannelFactory() {
    //create connection
    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("localhost");
    factory.setHost("ec2-100-25-150-0.compute-1.amazonaws.com");
    factory.setPort(5672);
    factory.setUsername("admin");
    factory.setPassword("password");

    try {
      conn = factory.newConnection();
      System.out.print("connection successful");
    } catch (Exception e) {
      System.out.print(e);
    }
  }

  @Override
  public Channel create() throws IOException {
    return conn.createChannel();
  }

  @Override
  public PooledObject<Channel> wrap(Channel channel) {
    return new DefaultPooledObject<>(channel);
  }
}
