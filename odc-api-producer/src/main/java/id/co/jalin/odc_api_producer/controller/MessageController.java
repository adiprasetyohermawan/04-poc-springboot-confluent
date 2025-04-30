package id.co.jalin.odc_api_producer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;


@RestController
// public class MessageController {
//     @GetMapping("/ping")
//     public String ping() {
//         return "pong";
//     }
// }

@RequestMapping("/api")
@RequiredArgsConstructor
public class MessageController {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String DEV_API_TOPIC = "dev_api";

    @PostMapping("/messages")
    public String postNestedJson(@RequestBody String payload) {
        kafkaTemplate.send(DEV_API_TOPIC, payload);
        return "Successfully sent nested JSON to topic: " + DEV_API_TOPIC;
    }

    // @PostMapping("/send")
    // public String sendMessage(@RequestParam String topic, @RequestParam String message) {
    //     kafkaTemplate.send(topic, message);
    //     return "Message sent to topic " + topic;
    // }
}   

