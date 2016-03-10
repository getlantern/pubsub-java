package org.getlantern.pubsub;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

/**
 * A client for Lantern's pubsub infrastructure.
 */
public class Client implements Runnable {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static final Runnable NOOP = new Runnable() {
        public void run() {
        };
    };

    private final ClientConfig cfg;
    private final LinkedBlockingQueue<Runnable> outQueue =
            new LinkedBlockingQueue<Runnable>(1);
    private final LinkedBlockingQueue<Message> in =
            new LinkedBlockingQueue<Message>(1);
    private final AtomicBoolean forceReconnect = new AtomicBoolean();
    private volatile Socket socket;
    private volatile MessagePacker packer;
    private final ScheduledExecutorService scheduledExecutor = Executors
            .newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> nextKeepalive;

    public static class ClientConfig {
        public Callable<Socket> dial;
        public long backoffBase;
        public long maxBackoff;
        public long keepalivePeriod;
        public String authenticationKey;
        public byte[][] initialTopics;
    }

    public Client(ClientConfig cfg) {
        // Apply sensible defaults
        if (cfg.backoffBase == 0) {
            cfg.backoffBase = 1000; // 1 second
        }
        if (cfg.maxBackoff == 0) {
            cfg.maxBackoff = 60 * 1000; // 1 minute
        }
        if (cfg.keepalivePeriod == 0) {
            cfg.keepalivePeriod = 30 * 1000; // 30 seconds
        }

        this.cfg = cfg;
        Thread thread = new Thread(this, "Client");
        thread.setDaemon(true);
        thread.start();
    }

    public static byte[] utf8(String str) {
        return str.getBytes(UTF8);
    }

    public Message read() throws InterruptedException {
        return in.take();
    }

    public Message readTimeout(long timeout, TimeUnit unit)
            throws InterruptedException {
        return in.poll(timeout, unit);
    }

    public Sendable subscribe(byte[] topic) {
        return new Sendable(this, new Message(Type.Subscribe, topic, null));
    }

    public Sendable unsubscribe(byte[] topic) {
        return new Sendable(this, new Message(Type.Unsubscribe, topic, null));
    }

    public Sendable publish(byte[] topic, byte[] body) {
        return new Sendable(this, new Message(Type.Publish, topic, body));
    }

    private Sendable authenticate(String authenticationKey) {
        return new Sendable(this, new Message(Type.Authenticate, null,
                utf8(authenticationKey)));
    }

    private Sendable keepAlive() {
        return new Sendable(this, new Message(Type.KeepAlive, null, null));
    }

    public void run() {
        forceConnect();
        try {
            process();
        } catch (InterruptedException ie) {
            throw new RuntimeException("Interrupted while processing", ie);
        }
    }

    private void process() throws InterruptedException {
        while (true) {
            doWithConnection(outQueue.take());
        }
    }

    private void doWithConnection(Runnable op) throws InterruptedException {
        for (int numFailures = 0; numFailures < Integer.MAX_VALUE; numFailures++) {
            // Back off if necessary
            if (numFailures > 0) {
                long backoff = (long) (Math.pow(cfg.backoffBase, numFailures));
                backoff = Math.min(backoff, cfg.maxBackoff);
                Thread.sleep(backoff);
            }

            if (socket == null || forceReconnect.compareAndSet(true, false)) {
                close();
                
                // Dial
                try {
                    System.err.println("Dialing");
                    socket = cfg.dial.call();
                    packer = MessagePack
                            .newDefaultPacker(new BufferedOutputStream(socket
                                    .getOutputStream()));
                    System.err.println("Sending initial messages");
                    sendInitialMessages();

                    System.err.println("Starting read loop");
                    final InputStream in = socket.getInputStream();
                    // Start read loop
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            readLoop(MessagePack.newDefaultUnpacker(new BufferedInputStream(
                                    in)));
                        }
                    }, "Client-ReadLoop");
                    thread.setDaemon(true);
                    thread.start();

                    System.err.println("Success");
                    // Success
                    return;
                } catch (Exception e) {
                    e.printStackTrace();
                    close();
                    continue;
                }
            }

            try {
                // Run the op
                op.run();

                // Success
                return;
            } catch (Exception e) {
                e.printStackTrace();
                close();
            }
        }
    }

    private void sendInitialMessages() throws IOException, InterruptedException {
        if (cfg.authenticationKey != null) {
            authenticate(cfg.authenticationKey).sendImmediate();
        }

        if (cfg.initialTopics != null) {
            for (byte[] topic : cfg.initialTopics) {
                subscribe(topic).sendImmediate();
            }
        }
    }

    private void readLoop(MessageUnpacker in) {
        try {
            doReadLoop(in);
        } catch (Exception e) {
            System.err.println("Error reading, closing connection");
            forceReconnect.set(true);
            forceConnect();
        }
    }

    private void doReadLoop(MessageUnpacker in) throws IOException,
            InterruptedException {
        while (true) {
            Message message = new Message();
            message.setType(in.unpackByte());
            message.setTopic(unpackByteArray(in));
            message.setBody(unpackByteArray(in));
            this.in.put(message);
        }
    }

    private void forceConnect() {
        outQueue.offer(NOOP);
    }

    private final Runnable sendKeepalive = new Runnable() {
        public void run() {
            outQueue.offer(keepAlive());
        }
    };

    private synchronized void resetKeepalive() {
        if (nextKeepalive != null) {
            nextKeepalive.cancel(false);
        }
        scheduledExecutor.schedule(sendKeepalive, cfg.keepalivePeriod,
                TimeUnit.MILLISECONDS);
    }

    private void close() {
        if (socket != null) {
            System.err.println("Closing socket");
            try {
                socket.close();
            } catch (Exception e) {
                // ignore exception on close
            }
            socket = null;
        }
    }

    public static class Sendable implements Runnable {
        private Client client;
        private Message msg;

        public Sendable(Client client, Message msg) {
            super();
            this.client = client;
            this.msg = msg;
        }

        public void send() throws InterruptedException {
            client.outQueue.put(this);
        }

        public void run() {
            try {
                sendImmediate();
            } catch (IOException ioe) {
                throw new RuntimeException("Unable to send message: "
                        + ioe.getMessage(), ioe);
            }
        }

        private void sendImmediate() throws IOException {
            client.resetKeepalive();
            client.packer.packByte(msg.getType());
            packByteArray(msg.getTopic());
            packByteArray(msg.getBody());
            client.packer.flush();
        }

        private void packByteArray(byte[] bytes) throws IOException {
            if (bytes == null) {
                client.packer.packNil();
            } else {
                client.packer.packBinaryHeader(bytes.length);
                client.packer.writePayload(bytes);
            }
        }
    }

    private static byte[] unpackByteArray(MessageUnpacker in)
            throws IOException {
        if (MessageFormat.NIL == in.getNextFormat()) {
            return null;
        }
        int length = in.unpackBinaryHeader();
        byte[] result = new byte[length];
        in.readPayload(result);
        return result;
    }
}
