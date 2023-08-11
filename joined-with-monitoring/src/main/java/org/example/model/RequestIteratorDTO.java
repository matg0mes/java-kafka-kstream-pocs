package org.example.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.example.enums.RequestTypeEnums;

import java.util.*;
import java.util.stream.Collectors;

public class RequestIteratorDTO {

    List<RequestDTO> requests;

    public RequestIteratorDTO() {
        this.requests = new ArrayList<>();
    }

    public static Boolean existsAllTypes(RequestIteratorDTO dto) {
        return new HashSet<>(dto.getRequests().stream()
                .filter(Objects::nonNull)
                .map(RequestDTO::getTopicEnums)
                .collect(Collectors.toList())).containsAll(Arrays.asList(RequestTypeEnums.values()));
    }

    public static class RequestDTOSerializer implements Serializer<RequestIteratorDTO> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, RequestIteratorDTO data) {

            if (Objects.isNull(data)) {
                return null;
            }
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing message",
                        e);
            }
        }

        @Override
        public void close() {

        }
    }

    public static class RequestDTODeserializer implements Deserializer<RequestIteratorDTO> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public RequestIteratorDTO deserialize(String topic, byte[] bytes) {
            if (Objects.isNull(bytes)) {
                return null;
            }

            RequestIteratorDTO data = null;
            try {
                data = objectMapper.treeToValue(objectMapper.readTree(bytes),
                        RequestIteratorDTO.class);
            } catch (Exception e) {
                System.out.println(e);
            }

            return data;
        }

        @Override
        public void close() {

        }
    }

    public List<RequestDTO> getRequests() {
        return requests;
    }

    public void setRequests(List<RequestDTO> requests) {
        this.requests = requests;
    }
}
