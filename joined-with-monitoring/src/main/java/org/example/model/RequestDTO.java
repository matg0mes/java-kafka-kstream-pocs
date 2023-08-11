package org.example.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.example.enums.RequestTypeEnums;

import java.util.*;
import java.util.stream.Collectors;

public class RequestDTO {

    private RequestTypeEnums topicEnums;
    private String description;

    public static Boolean existsAllTypes(List<RequestDTO> dto) {
        return new HashSet<>(Arrays.asList(RequestTypeEnums.values())).containsAll(dto.stream().map(RequestDTO::getTopicEnums)
                .collect(Collectors.toList()));
    }


    public static class RequestDTOSerializer implements Serializer<RequestDTO> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public byte[] serialize(String topic, RequestDTO data) {

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

    public static class RequestDTODeserializer implements Deserializer<RequestDTO> {

        private ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {

        }

        @Override
        public RequestDTO deserialize(String topic, byte[] bytes) {
            if (Objects.isNull(bytes)) {
                return null;
            }

            RequestDTO data;
            try {
                data = objectMapper.treeToValue(objectMapper.readTree(bytes),
                        RequestDTO.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }

            return data;
        }

        @Override
        public void close() {

        }
    }



    public RequestTypeEnums getTopicEnums() {
        return topicEnums;
    }

    public String getDescription() {
        return description;
    }

    public void setTopicEnums(RequestTypeEnums topicEnums) {
        this.topicEnums = topicEnums;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
