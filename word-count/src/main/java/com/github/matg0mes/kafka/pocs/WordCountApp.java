package com.github.matg0mes.kafka.pocs;

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

public class WordCountApp {
    public static void main(String[] args) {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-pocs");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        //Streams from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        //map values to lower case
        //can be alternatively writen as
        //.mapValue(String::toLowerCase);
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(textLine -> textLine.toLowerCase())

                //flatmap values split by space
                .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))

                // select key to apply a key (we discard the old key)
                .selectKey((ignoredKey, word) -> word)

                // group by key before aggregation
                .groupByKey()
                //Count occurrences
                .count(Named.as("Counts"));

        //To in order to write the results back to kafka
        wordCounts.toStream().to("word-count-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}