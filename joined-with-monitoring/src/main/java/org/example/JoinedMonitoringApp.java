package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.example.model.RequestDTO;
import org.example.model.RequestIteratorDTO;
import org.example.service.RequestJoinerService;

import java.time.Duration;
import java.util.Properties;

public class JoinedMonitoringApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreamadada-dadmamasa");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");

        StreamsBuilder builder = new StreamsBuilder();

        Serde<RequestDTO> requestDTOJsonSerde = Serdes
                .serdeFrom(new RequestDTO.RequestDTOSerializer(), new RequestDTO.RequestDTODeserializer());
        Serde<RequestIteratorDTO> requestDTOIteratorJsonSerde = Serdes
                .serdeFrom(new RequestIteratorDTO.RequestDTOSerializer(), new RequestIteratorDTO.RequestDTODeserializer());

        Consumed<String, RequestDTO> consumed = Consumed.with(Serdes.String(), requestDTOJsonSerde);

        // KTable cause maintain consistence and not lose performance using stre
        KTable<String, RequestDTO> topic1 = builder.table("topic-fragment-1", consumed);
        KTable<String, RequestDTO> topic2 = builder.table("topic-fragment-2", consumed);
        KTable<String, RequestDTO> topic3 = builder.table("topic-fragment-3", consumed);

        KTable<String, RequestIteratorDTO> joined = topic1.outerJoin(topic2,
                new RequestJoinerService.RequestJoinerObject()
        ).outerJoin(topic3,
                new RequestJoinerService.RequestJoinerIncrement()
        );

        joined.toStream().to("topic-joined.source", Produced.with(Serdes.String(), requestDTOIteratorJsonSerde));

        joined.filter((ignoredKey, value) -> RequestIteratorDTO.existsAllTypes(value))
                .toStream().to("topic-joi", Produced.with(Serdes.String(), requestDTOIteratorJsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}