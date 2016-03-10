package org.getlantern.pubsub;

import java.net.Socket;
import java.util.concurrent.Callable;

import org.getlantern.pubsub.Client.ClientConfig;

public class BaseClient {
    public static final String SERVER = "pubsub.lantern.io";
    public static final int PORT = 443;
    public static final byte[] TOPIC = Client.utf8("topic");
    public static final byte[] BODY = Client.utf8("this is the message body");

    protected static Client newClient(String authenticationKey,
            byte[]... initialTopics) {
        ClientConfig cfg = new ClientConfig();
        cfg.dial = new Callable<Socket>() {
            public Socket call() throws Exception {
                return new Socket(SERVER, PORT);
            }
        };
        cfg.authenticationKey = authenticationKey;
        cfg.backoffBase = 100;
        cfg.maxBackoff = 15000;
        cfg.initialTopics = initialTopics;
        return new Client(cfg);
    }
}
