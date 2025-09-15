import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

public class SolaceJmsPublisher {
    public static void main(String[] args) throws Exception {
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.solacesystems.jndi.SolJNDIInitialContextFactory");
        env.put(Context.PROVIDER_URL, "smf://HOST:55555");
        env.put(Context.SECURITY_PRINCIPAL, "USERNAME");
        env.put(Context.SECURITY_CREDENTIALS, "PASSWORD");
        env.put("connectionFactory.myCF", "smf://HOST:55555");
        env.put("topic.myTopic", "example/topic");

        InitialContext ctx = new InitialContext(env);
        TopicConnectionFactory cf = (TopicConnectionFactory) ctx.lookup("myCF");
        Topic topic = (Topic) ctx.lookup("myTopic");

        TopicConnection conn = cf.createTopicConnection();
        conn.start();
        TopicSession session = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

        TopicPublisher publisher = session.createPublisher(topic);
        TextMessage msg = session.createTextMessage("Hello from JMS inside Docker");
        publisher.publish(msg);

        System.out.println("Published message.");
        conn.close();
    }
}
