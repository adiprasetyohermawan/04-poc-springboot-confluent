package id.co.jalin.odc_api_producer.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import id.co.jalin.odc_api_producer.dto.OutputRecord;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;


@RestController
// public class MessageController {
//     @GetMapping("/ping")
//     public String ping() {
//         return "pong";
//     }
// }

@RequestMapping("/api")
@RequiredArgsConstructor
public class MessageController
 {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private static final String DEV_API_TOPIC = "dev_api";
    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final DateTimeFormatter RECEIVED_FMT =DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");



    @PostMapping("/messages")
    public String postNestedJson(@RequestBody String payload) {
        kafkaTemplate.send(DEV_API_TOPIC, payload);

         // 1. Encode the raw JSON string to Base64
        String encoded = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));

        // 2. Send the Base64 payload to Kafka
        kafkaTemplate.send(DEV_API_TOPIC, encoded);

        return "Successfully sent nested JSON to topic: " + DEV_API_TOPIC;

    }   

@PostMapping("/send")
    // public String sendMessage(@RequestParam String topic, @RequestParam String message) {
    public ResponseEntity<OutputRecord> sendMessage( @RequestBody String payload) throws Exception {
        JsonNode root = objectMapper.readTree(payload);
        
        // 1) Generate or extract each field
        long systemId = COUNTER.incrementAndGet();
        String source            = root.path("source").asText("");
        String sourceId          = root.path("source_id").asText("");
        String dataKey           = root.path("tieredData")
                                       .path("fields")
                                       .path("UUID")
                                       .path("value")
                                       .asText("");
        String refDataKey        = root.path("tieredData")
                                       .path("fields")
                                       .path("REFERENCE_NUMBER")
                                       .path("value")
                                       .asText("");
        String dataIdentifier    = root.path("tieredData")
                                       .path("fields")
                                       .path("MC")
                                       .path("value")
                                       .asText("");
        String rawDataB64        = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
        String receivedDate      = LocalDateTime.now().format(RECEIVED_FMT);

        // 2) Build the record
        OutputRecord rec = new OutputRecord();
        rec.setSystemId(systemId);
        rec.setReferenceSystemId("");
        rec.setSource(source);
        rec.setSourceId(sourceId);
        rec.setSourceDate("");
        rec.setReceivedDate(receivedDate);
        rec.setDataKey(dataKey);
        rec.setReferenceDataKey(refDataKey);
        rec.setDataIdentifier(dataIdentifier);
        rec.setRawData(rawDataB64);
        rec.setReasonUnprocessed("");


        


        // 3) Serialize & send to Kafka
        String jsonOut = objectMapper.writeValueAsString(rec);
        // kafkaTemplate.send(DEV_API_TOPIC, jsonOut);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(DEV_API_TOPIC, jsonOut);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Error sending message: " + ex.getMessage());
            } else {
                System.out.println("Message sent successfully: " + result.getProducerRecord().value());
            }
        });


        // 4) Return the same record to the caller    
        return ResponseEntity.ok(rec);
    }

        


//     }
}

