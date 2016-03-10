package org.getlantern.pubsub;

public class LoadGeneratingClient extends BaseClient {
    private static long interval = 100000;

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Please specify an authentication key");
            System.exit(1);
        }

        Client client = newClient(args[0]);
        long start = System.currentTimeMillis();
        for (long i = 0; i < Long.MAX_VALUE; i++) {
            client.publish(TOPIC, BODY).send();
            if (i % interval == 0 && i > 0) {
                double delta = System.currentTimeMillis() - start;
                System.out.println("" + (interval * 1000.0 / delta) + "tps");
                start = System.currentTimeMillis();
            }
        }
    }
}
