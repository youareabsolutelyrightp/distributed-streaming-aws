package dev.portfolio.taxi.input;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;

public final class InputPartitioning {
    public static final String VERSION = "taxi-medallion-partition-v1";
    public static final String HASH_VECTOR_VERSION = "sha256-prefix64-be-v1";
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final ConcurrentHashMap<String, byte[]> DATASET_BYTES =
            new ConcurrentHashMap<>();
    private static final ThreadLocal<MessageDigest> SHA256 = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    });

    private InputPartitioning() {}

    public static int partition(String medallion, int partitionCount) {
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partition count must be positive");
        }
        byte[] digest = sha256(medallion.getBytes(StandardCharsets.UTF_8));
        long prefix = ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
        return (int) Long.remainderUnsigned(prefix, partitionCount);
    }

    public static String expectedEventId(String datasetId, String joinedRecordId) {
        byte[] digest = eventDigest(datasetId, joinedRecordId);
        char[] text = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            int value = digest[index] & 0xff;
            text[index * 2] = HEX[value >>> 4];
            text[index * 2 + 1] = HEX[value & 0x0f];
        }
        return new String(text);
    }

    public static boolean eventIdMatches(
            String datasetId, String joinedRecordId, String eventId) {
        if (eventId == null || eventId.length() != 64) {
            return false;
        }
        byte[] digest = eventDigest(datasetId, joinedRecordId);
        for (int index = 0; index < digest.length; index++) {
            int value = digest[index] & 0xff;
            if (eventId.charAt(index * 2) != HEX[value >>> 4]
                    || eventId.charAt(index * 2 + 1) != HEX[value & 0x0f]) {
                return false;
            }
        }
        return true;
    }

    private static byte[] eventDigest(String datasetId, String joinedRecordId) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        digest.update(DATASET_BYTES.computeIfAbsent(
                datasetId, value -> value.getBytes(StandardCharsets.UTF_8)));
        return digest.digest(joinedRecordId.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) {
        MessageDigest digest = SHA256.get();
        digest.reset();
        return digest.digest(value);
    }
}
