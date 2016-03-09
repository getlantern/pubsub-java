package org.getlantern.pubsub;

import static org.junit.Assert.*;

import java.net.Socket;
import java.util.concurrent.Callable;

import org.getlantern.pubsub.Client.ClientConfig;
import org.junit.Test;

public class ClientTest {
    @Test
    public void testRoundTrip() throws Exception {
        byte[] topic = Client.utf8("topic");
        byte[] body = Client.utf8("body");
        ClientConfig cfg = new ClientConfig();
        cfg.dial = new Callable<Socket>() {
            public Socket call() throws Exception {
                return new Socket("localhost", 14080);
            }
        };
        cfg.authenticationKey = "test";
        cfg.initialTopics = new byte[][] { topic };
        Client client = new Client(cfg);
        client.publish(topic, body).send();
        System.err.println("Published");
        Message msg = client.read();
        assertArrayEquals(topic, msg.getTopic());
        assertArrayEquals(body, msg.getBody());
    }
}
