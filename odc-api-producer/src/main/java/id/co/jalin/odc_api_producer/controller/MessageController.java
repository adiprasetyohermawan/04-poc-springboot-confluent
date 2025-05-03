package id.co.jalin.odc_api_producer.controller;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPOutputStream;

@RestController
@RequestMapping("/api")
public class MessageController {

    @Autowired
    private KafkaTemplate<String, GenericRecord> kafkaTemplate;

    private static final String DEV_API_TOPIC = "dev_api";
    private static final AtomicLong COUNTER = new AtomicLong(0);
    private static final DateTimeFormatter RECEIVED_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SCHEMA_STRING = """
        {
          "type": "record",
          "name": "KafkaPayload",
          "namespace": "com.jalin.api_odc.avro",
          "fields": [
            {"name": "system_id", "type": "long"},
            {"name": "reference_system_id", "type": ["null", "string"], "default": null},
            {"name": "source", "type": ["null", "string"], "default": null},
            {"name": "source_id", "type": ["null", "string"], "default": null},
            {"name": "source_date", "type": ["null", "string"], "default": null},
            {"name": "received_datetime", "type": ["null", "string"], "default": null},
            {"name": "data_key", "type": ["null", "string"], "default": null},
            {"name": "reference_data_key", "type": ["null", "string"], "default": null},
            {"name": "data_identifier", "type": ["null", "string"], "default": null},
            {"name": "raw_data", "type": ["null", "string"], "default": null},
            {"name": "reason_unprocessed", "type": ["null", "string"], "default": null}
          ]
        }
        """;

    @PostMapping("/odc")
    public ResponseEntity<String> sendMessage(@RequestBody String payload) throws Exception {
        JsonNode root = objectMapper.readTree(payload);

        long systemId = COUNTER.incrementAndGet();
        String source = root.path("tieredData").path("fields").path("SOURCE").path("value").asText(null);
        String sourceId = root.path("tieredData").path("fields").path("SOURCE_ID").path("value").asText(null);
        String dataKey = root.path("tieredData").path("fields").path("UUID").path("value").asText(null);
        String refDataKey = root.path("tieredData").path("fields").path("REFERENCE_NUMBER").path("value").asText(null);
        String dataIdentifier = root.path("tieredData").path("fields").path("MC").path("value").asText(null);
        String rawDataB64 = encodeGzipBase64(payload);
        String receivedDate = LocalDateTime.now().format(RECEIVED_FMT);

        // Build Avro schema and GenericRecord
        Schema schema = new Schema.Parser().parse(SCHEMA_STRING);
        GenericRecord record = new GenericData.Record(schema);

        record.put("system_id", systemId);
        record.put("reference_system_id", null);
        record.put("source", source);
        record.put("source_id", sourceId);
        record.put("source_date", null);
        record.put("received_datetime", receivedDate);
        record.put("data_key", dataKey);
        record.put("reference_data_key", refDataKey);
        record.put("data_identifier", dataIdentifier);
        record.put("raw_data", rawDataB64);
        record.put("reason_unprocessed", null);

        // Send using Kafka
        CompletableFuture<SendResult<String, GenericRecord>> future = kafkaTemplate.send(DEV_API_TOPIC, record);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                System.err.println("Failed to send message: " + ex.getMessage());
            } else {
                System.out.println("Message sent: " + result.getRecordMetadata().offset());
            }
        });

        return ResponseEntity.ok("Message sent to Kafka topic: " + DEV_API_TOPIC);
    }

    private String encodeGzipBase64(String json) {
        try {
            // 1. Minify JSON string using Jackson
            JsonNode tree = objectMapper.readTree(json);
            String compactJson = objectMapper.writeValueAsString(tree);

            // 2. Compress using GZIP
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            try (GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
                gzipStream.write(compactJson.getBytes(StandardCharsets.UTF_8));
            }

            // 3. Encode to URL-safe Base64 **without padding**
            String base64 = Base64.getUrlEncoder()
                                .withoutPadding()
                                .encodeToString(byteStream.toByteArray());

            // 4. Prefix with "GZ"
            return "GZ" + base64;

        } catch (Exception e) {
            throw new RuntimeException("Failed to compress and encode payload", e);
        }
    }
    
}