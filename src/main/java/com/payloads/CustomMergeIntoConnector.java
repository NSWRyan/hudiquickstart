package com.payloads;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;

import java.io.IOException;
import java.util.Properties;

public class CustomMergeIntoConnector extends OverwriteWithLatestAvroPayload {
    public CustomMergeIntoConnector(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public CustomMergeIntoConnector(Option<GenericRecord> record) {
        this(record.isPresent() ? record.get() : null, 0); // natural order
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema,
            Properties properties) throws IOException {
        if (recordBytes.length == 0) {
            return Option.empty();
        }

        GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);

        // Custom code: Preserve "ts" and "rider" "driver" from currentValue and add
        // fare adjustment
        if (currentValue instanceof GenericRecord) {
            Object currentTs = ((GenericRecord) currentValue).get("ts");
            Object currentRider = ((GenericRecord) currentValue).get("rider");
            Object currentDriver = ((GenericRecord) currentValue).get("driver");
            Double currentFare = (Double) (((GenericRecord) currentValue).get("fare"));
            Double fareAdjustment = (Double) (((GenericRecord) incomingRecord).get("fare"));
            if (currentTs != null) {
                incomingRecord.put("ts", currentTs);
            }
            if (currentRider != null) {
                incomingRecord.put("rider", currentRider);
            }
            if (currentDriver != null) {
                incomingRecord.put("driver", currentDriver);
            }
            if (currentFare != null && fareAdjustment != null) {
                incomingRecord.put("fare", currentFare + fareAdjustment);
            } else if (currentFare != null) {
                incomingRecord.put("fare", currentFare);
            }
        }

        return isDeleteRecord(incomingRecord) ? Option.empty() : Option.of(incomingRecord);
    }
}