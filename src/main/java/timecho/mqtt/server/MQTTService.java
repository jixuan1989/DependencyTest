package timecho.mqtt.server;

import com.librato.metrics.client.Lists;
import io.moquette.BrokerConstants;
import io.moquette.broker.Server;
import io.moquette.broker.config.IConfig;
import io.moquette.broker.config.MemoryConfig;
import io.moquette.broker.security.IAuthenticator;
import io.moquette.interception.InterceptHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class MQTTService {
  private static final Logger LOG = LoggerFactory.getLogger(MQTTService.class);
  private Server server = new Server();

  private MQTTService() {}

  public static void main(String[] args) {
    String ip = "127.0.0.1";
    String port = "1883";
    if (args.length == 2) {
      ip = args[0];
      port = args[1];
    } else if (args.length == 1) {
      ip = args[0];
    }
    getInstance().startup(ip, port);
  }


  public void startup(String ip, String port) {
    IConfig config = createBrokerConfig(ip, port);
    List<InterceptHandler> handlers = Collections.singletonList(new PublishHandler());
    IAuthenticator authenticator = new BrokerAuthenticator();

    server.startServer(config, handlers, null, authenticator, null);

    LOG.info(
        "Start MQTT service successfully, listening on ip {} port {}",
        ip,
        port);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOG.info("Stopping IoTDB MQTT service...");
                  shutdown();
                  LOG.info("IoTDB MQTT service stopped.");
                }));
  }

  private IConfig createBrokerConfig(String ip, String port) {
    Properties properties = new Properties();
    properties.setProperty(BrokerConstants.HOST_PROPERTY_NAME, ip);
    properties.setProperty(
        BrokerConstants.PORT_PROPERTY_NAME, port);
    properties.setProperty(
        BrokerConstants.BROKER_INTERCEPTOR_THREAD_POOL_SIZE,
        String.valueOf(1));
    properties.setProperty(BrokerConstants.IMMEDIATE_BUFFER_FLUSH_PROPERTY_NAME, "true");
    properties.setProperty(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, "false");
    properties.setProperty(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, "true");
    properties.setProperty(
        BrokerConstants.NETTY_MAX_BYTES_PROPERTY_NAME,
        String.valueOf(1048576));
    return new MemoryConfig(properties);
  }

  public void shutdown() {
    server.stopServer();
  }

  public static MQTTService getInstance() {
    return MQTTServiceHolder.INSTANCE;
  }

  private static class MQTTServiceHolder {

    private static final MQTTService INSTANCE = new MQTTService();

    private MQTTServiceHolder() {}
  }
}
