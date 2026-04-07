package com.taller;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class Consumer {

    private static final String EXCHANGE_NAME = "events";
    private static final String QUEUE_NAME    = "queue.java";
    private static final String BINDING_KEY   = "user.#";

    private static String getenv(String key, String fallback) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : fallback;
    }

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        String host = getenv("RABBITMQ_HOST", "rabbitmq");
        int    port = Integer.parseInt(getenv("RABBITMQ_PORT", "5672"));
        String user = getenv("RABBITMQ_USER", "guest");
        String pass = getenv("RABBITMQ_PASS", "guest");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(user);
        factory.setPassword(pass);

        Connection connection = null;
        int retries = 10;
        for (int i = 1; i <= retries; i++) {
            try {
                connection = factory.newConnection();
                break;
            } catch (Exception e) {
                System.out.printf("[Java Consumer] Attempt %d/%d failed: %s. Retrying in 3s...%n", i, retries, e.getMessage());
                Thread.sleep(3000);
            }
        }

        if (connection == null) {
            System.err.println("[Java Consumer] Could not connect to RabbitMQ. Exiting.");
            System.exit(1);
        }

        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, BINDING_KEY);
        channel.basicQos(1);

        System.out.printf("[Java Consumer] Waiting for messages on queue='%s' binding='%s'...%n",
                QUEUE_NAME, BINDING_KEY);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String body = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("[Java Consumer] Received: " + body);
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});

        // Keep thread alive
        Thread.currentThread().join();
    }
}
