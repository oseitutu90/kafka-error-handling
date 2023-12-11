package com.quicklearninghub.retry.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.quicklearninghub.retry.dto.MyDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


import java.util.*;
@Service
@Slf4j
public class RetryMessageListener {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, MyDTO> kafkaTemplate;
    private final String retryTopic;

    @Autowired
    public RetryMessageListener(ObjectMapper objectMapper,
                                KafkaTemplate<String, MyDTO> kafkaTemplate,
                                @Value("${kafka.topic}") String retryTopic) {
        this.objectMapper = objectMapper;
        this.kafkaTemplate = kafkaTemplate;
        this.retryTopic = retryTopic;
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void listen(ConsumerRecord<String, String> consumerRecord) {
        log.info("Started consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(),
                consumerRecord.offset(), consumerRecord.value());

        if (consumerRecord.offset() % 2 != 0) {
            throw new IllegalStateException("This is something odd.");
        }

        try {
            MyDTO myDto = objectMapper.readValue(consumerRecord.value(), MyDTO.class);
            log.info("Finished consuming message on topic: {}, offset {}, message {}", consumerRecord.topic(),
                    consumerRecord.offset(), myDto);

            // Perform some processing with the deserialized object

            // Example: Sending the deserialized object to another topic
            kafkaTemplate.send(retryTopic, myDto);
        } catch (Exception e) {
            log.error("Error processing message on topic: {}, offset {}", consumerRecord.topic(),
                    consumerRecord.offset(), e);
        }
    }
}
