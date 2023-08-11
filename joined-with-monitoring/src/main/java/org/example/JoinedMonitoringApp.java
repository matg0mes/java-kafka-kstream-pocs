package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.example.model.RequestDTO;
import org.example.service.RequestJoinerService;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class JoinedMonitoringApp {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-pocs1235");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC, "0");

        StreamsBuilder builder = new StreamsBuilder();

        Serde<RequestDTO> requestDTOJsonSerde = Serdes.serdeFrom(new RequestDTO.RequestDTOSerializer(), new RequestDTO.RequestDTODeserializer());

        Consumed<String, RequestDTO> consumed = Consumed.with(Serdes.String(), requestDTOJsonSerde);

        KStream<String, RequestDTO> topic1 = builder.stream("topic-fragment-1", consumed);
        KStream<String, RequestDTO> topic2 = builder.stream("topic-fragment-2", consumed);
        KStream<String, RequestDTO> topic3 = builder.stream("topic-fragment-3", consumed);

        KStream<String, List<RequestDTO>> joined = topic1.outerJoin(topic2,
                new RequestJoinerService.RequestJoinerObject(),
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(60), Duration.ofMinutes(1)),
                StreamJoined.with(Serdes.String(), requestDTOJsonSerde, requestDTOJsonSerde)
        ).outerJoin(topic3,
                new RequestJoinerService.RequestJoinerIncrement(),
                JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(60), Duration.ofMinutes(1)));

        joined.filter((ignoredKey, value) -> RequestDTO.existsAllTypes(value))
                .to("topic-joined");

        joined.filterNot((ignoredKey, value) -> RequestDTO.existsAllTypes(value))
                .to("topic-monitoring-joined");


        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}