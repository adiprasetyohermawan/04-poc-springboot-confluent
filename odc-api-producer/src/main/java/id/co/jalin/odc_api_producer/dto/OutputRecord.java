package id.co.jalin.odc_api_producer.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OutputRecord {
    @JsonProperty("system_id")
    private long systemId;

    @JsonProperty("reference_system_id")
    private String referenceSystemId;

    @JsonProperty("source")
    private String source;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("source_date")
    private String sourceDate;

    @JsonProperty("received_date")
    private String receivedDate;

    @JsonProperty("data_key")
    private String dataKey;

    @JsonProperty("reference_data_key")
    private String referenceDataKey;

    @JsonProperty("data_identifier")
    private String dataIdentifier;

    @JsonProperty("raw_data")
    private String rawData;

    @JsonProperty("reason_unprocessed")
    private String reasonUnprocessed;
}
