package dev.portfolio.taxi.ranking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;

import org.junit.jupiter.api.Test;

final class VirtualShardsTest {
    @Test
    void stableHashHasFixedTestVectors() {
        Map<Long, Long> vectors = Map.of(
                0L, 0L,
                1L, 6_238_072_747_940_578_789L,
                -1L, -5_417_735_806_833_148_549L,
                123_456_789L, -1_000_775_671_988_880_032L,
                Long.MAX_VALUE, 6_514_504_133_438_201_533L,
                Long.MIN_VALUE, 2_720_858_781_877_447_050L);

        vectors.forEach((input, expected) ->
                assertEquals(expected.longValue(), VirtualShards.mix64(input)));
    }
}
