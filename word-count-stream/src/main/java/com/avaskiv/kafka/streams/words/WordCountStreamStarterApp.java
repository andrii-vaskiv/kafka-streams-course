package com.avaskiv.kafka.streams.words;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamStarterApp {
    public static final String APPLICATION_NAME = "word-count";
    public static final String ADDRESS = "localhost:9092";
    public static final String OFFSET_RESET_TYPE = "earliest";
    public static final String OUTPUT_TOPIC = "word-count-output";
    public static final String INPUT_TOPIC = "word-count-input";

    public static void main(String[] args) {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> countWordsInput = builder.stream(INPUT_TOPIC);
        KTable<String, Long> countedWords = countWords(countWordsInput);
        sendToKafka(countedWords, OUTPUT_TOPIC);

        KafkaStreams streams = new KafkaStreams(builder.build(), config());
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static KTable<String, Long> countWords(KStream<String, String> inputStream) {
        KTable<String, Long> countedWords = inputStream.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                .selectKey((key, value) -> value)
                .groupByKey()
                .count(Named.as("Counts"));
        return countedWords;
    }

    public static void sendToKafka(KTable<String, Long> table, String outputTopicName) {
        table.toStream().to(outputTopicName);
    }


    public static Properties config() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ADDRESS);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_TYPE);
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        return config;
    }
}
