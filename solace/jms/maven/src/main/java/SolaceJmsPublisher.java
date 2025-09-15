import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class SolaceJmsPublisher {
    public static void main(String[] args) throws Exception {
        String host     = System.getenv().getOrDefault("SOLACE_HOST", "localhost");
        String username = System.getenv().getOrDefault("SOLACE_USERNAME", "default");
        String password = System.getenv().getOrDefault("SOLACE_PASSWORD", "default");
        String topicStr = System.getenv().getOrDefault("SOLACE_TOPIC", "test/topic");

        String smfUrl = "smf://" + host + ":55555";

        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.solacesystems.jndi.SolJNDIInitialContextFactory");
        env.put(Context.PROVIDER_URL, smfUrl);
        env.put(Context.SECURITY_PRINCIPAL, username);
        env.put(Context.SECURITY_CREDENTIALS, password);
        env.put("connectionFactory.myCF", smfUrl);
        env.put("topic.myTopic", topicStr);

        InitialContext ctx = new InitialContext(env);
        TopicConnectionFactory cf = (TopicConnectionFactory) ctx.lookup("myCF");
        Topic topic = (Topic) ctx.lookup("myTopic");

        TopicConnection conn = cf.createTopicConnection();
        conn.start();
        TopicSession session = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        TopicPublisher publisher = session.createPublisher(topic);
        TextMessage msg = session.createTextMessage("Hello from JMS + Maven + Docker + ENV");
        publisher.publish(msg);

        System.out.println("✅ Message published to topic: " + topicStr);
        conn.close();
    }
}
