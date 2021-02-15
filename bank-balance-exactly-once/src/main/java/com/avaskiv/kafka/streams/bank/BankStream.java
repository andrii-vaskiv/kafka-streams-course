package com.avaskiv.kafka.streams.bank;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Instant;
import java.util.Properties;

public class BankStream {
    public static final String APPLICATION_NAME = "bank-stream";
    public static final String ADDRESS = "localhost:9092";
    public static final String OFFSET_RESET_TYPE = "earliest";
    public static final String INPUT_TOPIC = "bank-transactions";
    public static final String OUTPUT_TOPIC = "bank-balance-exactly-once";
    public static final String AGGREGATE_TOPIC = "bank-aggregated-transactions";
    public static final String COUNT = "count";
    public static final String BALANCE = "balance";
    public static final String TIME = "time";


    public static void main(String[] args) {
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, JsonNode> bankTransactions = builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), jsonSerde));

        ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put(COUNT, 0);
        initialBalance.put(BALANCE, 0);
        initialBalance.put(TIME, Instant.ofEpochMilli(0L).toString());

        KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey()
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as(AGGREGATE_TOPIC).withKeySerde(Serdes.String()).withValueSerde(jsonSerde)
                );

        bankBalance.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config());
        streams.cleanUp();
        streams.start();

        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put(COUNT, balance.get(COUNT).asInt() + 1);
        newBalance.put(BALANCE, balance.get(BALANCE).asInt() + transaction.get("amount").asInt());

        long balanceEpoch = Instant.parse(balance.get(TIME).asText()).toEpochMilli();
        long transactionEpoch = Instant.parse(transaction.get(TIME).asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put(TIME, newBalanceInstant.toString());
        return newBalance;
    }


    private static Properties config() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ADDRESS);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_TYPE);
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return config;
    }
}
