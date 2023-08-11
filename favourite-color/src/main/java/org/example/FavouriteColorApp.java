package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class FavouriteColorApp {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-pocs1235");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");
        StreamsBuilder builder = new StreamsBuilder();
        //Streams from kafka

        List<String> whiteList = Arrays.asList("red", "blue", "green");

        KStream<String, String> textLines = builder.stream("favourite-color-input");

        KStream<String, String> userAndColours = textLines
                .filter((ignoredKey, word) -> word.contains(","))

                //Consigo alterar a key como eu quero
                .selectKey((ignoredKey, word) -> word.split(",")[0].toLowerCase())

                //Consigo alterar o value como eu quero
                .mapValues((ignoredKey, word) -> word.split(",")[1].toLowerCase())

                //Filtro as informações pelo que eu quero
                .filter((user, colors) -> whiteList.contains(colors));

        // Guardo o estado das operações em um topico
        userAndColours.to("user-and-colors");


        // Transformo o topico em KTable para realizar as operações de update, delete e create no topico
        KTable<String, String> usersAndColorsTable = builder.table("user-and-colors");

        KTable<String, Long> favouriteColors = usersAndColorsTable
                // Agrupo por cor
                .groupBy((user, color) -> new KeyValue<>(color, color))
                // Realizo a contagem da quantidade de cores
                .count(Named.as("CountByColors"));

        favouriteColors.toStream().to("favourite-color-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}