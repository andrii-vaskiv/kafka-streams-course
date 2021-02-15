package com.avaskiv.kafka.streams.bank;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankTransactionsProducer {
    public static final String ADDRESS = "localhost:9092";
    public static final String ACKS_CONFIG = "all";
    public static final String RETRIES_CONFIG = "3";
    public static final String LINGER_MS_CONFIG = "1";
    public static final String ENABLE_IDEMPOTENCE_CONFIG = "true";
    public static final String TOPIC = "bank-transactions";

    public static void main(String[] args) {
        Producer<String, String> producer = new KafkaProducer<>(config());
        int i = 0;

        while(true) {
            System.out.println("Producing batch " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("helga"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("ann"));
                Thread.sleep(100);
                i += 1;

            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    public static Properties config() {
        Properties config = new Properties();
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ADDRESS);
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        config.setProperty(ProducerConfig.RETRIES_CONFIG, RETRIES_CONFIG);
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, LINGER_MS_CONFIG);
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, ENABLE_IDEMPOTENCE_CONFIG);
        return config;
    }

    public static ProducerRecord<String, String> newRandomTransaction(String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        Instant now = Instant.now();
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(TOPIC, name, transaction.toString());
    }
}
