package org.example.service;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.model.RequestDTO;
import org.example.model.RequestIteratorDTO;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RequestJoinerService {

    public static class RequestJoinerObject implements ValueJoiner<RequestDTO, RequestDTO, RequestIteratorDTO> {
        @Override
        public RequestIteratorDTO apply(RequestDTO requestDTO, RequestDTO requestDTO2) {
            RequestIteratorDTO requestIteratorDTO = new RequestIteratorDTO();
            requestIteratorDTO.getRequests().addAll(Arrays.asList(requestDTO, requestDTO2));
            return requestIteratorDTO;
        }
    }

    public static class RequestJoinerIncrement implements ValueJoiner<RequestIteratorDTO, RequestDTO, RequestIteratorDTO> {
        @Override
        public RequestIteratorDTO apply(RequestIteratorDTO listRequests, RequestDTO requestDTO2) {
            listRequests.getRequests().add(requestDTO2);
            return listRequests;
        }
    }

}
