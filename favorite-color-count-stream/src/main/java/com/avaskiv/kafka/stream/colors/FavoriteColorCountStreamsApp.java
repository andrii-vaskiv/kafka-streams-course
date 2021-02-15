package com.avaskiv.kafka.stream.colors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class FavoriteColorCountStreamsApp {
    public static final String APPLICATION_NAME = "favorite-color-count";
    public static final String ADDRESS = "localhost:9092";
    public static final String OFFSET_RESET_TYPE = "earliest";
    public static final String OUTPUT_TOPIC = "favourite-colour-output";
    public static final String INPUT_TOPIC = "favourite-colour-input";
    public static final String USER_COLORS_COMPACT_TOPIC = "user-keys-and-colours";
    public static final List<String> ALLOWED_COLORS_LIST = Arrays.asList("red", "green", "blue");

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);

        KStream<String, String> usersAndColors = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> ALLOWED_COLORS_LIST.contains(color));

        usersAndColors.to(USER_COLORS_COMPACT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KTable<String, String> usersAndColorsTable = builder.table(USER_COLORS_COMPACT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, Long> favoriteColors = usersAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Named.as("CountsByColors"));

        favoriteColors.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config());
        streams.cleanUp();
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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
