package org.example.service;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.example.model.RequestDTO;

import java.util.Arrays;
import java.util.List;

public class RequestJoinerService {

    public static class RequestJoinerObject implements ValueJoiner<RequestDTO, RequestDTO, List<RequestDTO>> {
        @Override
        public List<RequestDTO> apply(RequestDTO requestDTO, RequestDTO requestDTO2) {
            return Arrays.asList(requestDTO, requestDTO2);
        }
    }

    public static class RequestJoinerIncrement implements ValueJoiner<List<RequestDTO>, RequestDTO, List<RequestDTO>> {
        @Override
        public List<RequestDTO> apply(List<RequestDTO> listRequests, RequestDTO requestDTO2) {
            listRequests.add(requestDTO2);
            return listRequests;
        }
    }

}
