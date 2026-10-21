package dev.portfolio.taxi.measurement;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

public final class TransportHeaders {
    public static final String TRACE_V3 = "transport_trace_v3";
    public static final int TRACE_V3_BYTES = 112;

    private TransportHeaders() {}

    public static Headers full(
            ResultTransport transport,
            String triggerType,
            boolean terminal,
            long serializationStartedEpochMicros,
            long serializationCompletedEpochMicros) {
        if (serializationCompletedEpochMicros < serializationStartedEpochMicros) {
            throw new IllegalArgumentException("serialization completion precedes start");
        }
        long emitted = orZero(transport.resultEmittedEpochMicros());
        ByteBuffer value = buffer(TRACE_V3_BYTES);
        value.put((byte) 3);
        value.put(flags(triggerType, terminal));
        value.put(partition(transport.triggerInputPartition()));
        value.put((byte) 0);
        value.putInt(0);
        value.putLong(orZero(transport.triggerInputSourceLine()));
        value.putLong(orZero(transport.boundaryFirstInputAppendEpochMicros()));
        value.putLong(orZero(transport.triggerInputAppendEpochMicros()));
        value.putLong(orZero(transport.triggerInputSourceReadEpochMicros()));
        value.putLong(orZero(transport.maximumRequiredSourceReadEpochMicros()));
        value.putLong(orZero(transport.partitionBoundaryEmittedEpochMicros()));
        value.putLong(orZero(transport.firstInputBoundaryArrivalEpochMicros()));
        value.putLong(orZero(transport.lastRequiredGlobalMessageArrivalEpochMicros()));
        value.putLong(orZero(transport.watermarkClosedEpochMicros()));
        value.putLong(orZero(transport.globalBoundaryStartedEpochMicros()));
        value.putLong(emitted);
        value.putInt(duration(orZero(transport.globalBoundaryStartedEpochMicros()), emitted));
        value.putInt(duration(emitted, serializationStartedEpochMicros));
        value.putInt(duration(serializationStartedEpochMicros, serializationCompletedEpochMicros));
        value.putInt(0);
        return new RecordHeaders().add(TRACE_V3, value.array());
    }

    private static ByteBuffer buffer(int size) {
        return ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
    }

    private static byte flags(String triggerType, boolean terminal) {
        int flags = terminal ? 4 : 0;
        if ("input".equals(triggerType) || "mixed".equals(triggerType)) flags |= 1;
        if ("event_time_timer".equals(triggerType) || "mixed".equals(triggerType)) flags |= 2;
        return (byte) flags;
    }

    private static byte partition(Integer value) {
        int partition = value == null ? -1 : value;
        if (partition < -1 || partition > 127) {
            throw new IllegalArgumentException("trigger partition does not fit transport header");
        }
        return (byte) partition;
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
    }

    private static int duration(long start, long end) {
        if (start == 0L && end == 0L) return 0;
        if (end < start || end - start > 0xffff_ffffL) {
            throw new IllegalArgumentException("transport duration is invalid");
        }
        return (int) (end - start);
    }

}
