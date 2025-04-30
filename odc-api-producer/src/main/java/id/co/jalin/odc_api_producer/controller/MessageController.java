package id.co.jalin.odc_api_producer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/message")
@RequiredArgsConstructor
public class MessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "dev_api";

    @PostMapping
    public String postNestedJson(@RequestBody String payload) {
        kafkaTemplate.send(TOPIC, payload);
        return "Successfully sent nested JSON to topic: " + TOPIC;
    }
}