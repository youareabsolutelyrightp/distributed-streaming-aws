package dev.portfolio.taxi.input;

import static dev.portfolio.taxi.input.InputModels.InputEnvelope;

import java.io.Serializable;

public record KafkaInputRecord(
        int kafkaPartition,
        long kafkaOffset,
        String rawValue,
        InputEnvelope envelope,
        String parseError)
        implements Serializable {

    public boolean valid() {
        return envelope != null;
    }
}
