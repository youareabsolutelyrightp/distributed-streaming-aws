package dev.portfolio.taxi.ranking;

public final class VirtualShards {
    private VirtualShards() {}

    public static int shard(long entityId, int virtualShards) {
        if (virtualShards <= 0) {
            throw new IllegalArgumentException("virtual shards must be positive");
        }
        return (int) Math.floorMod(mix64(entityId), (long) virtualShards);
    }

    public static long mix64(long value) {
        long mixed = value;
        mixed = (mixed ^ (mixed >>> 30)) * 0xbf58476d1ce4e5b9L;
        mixed = (mixed ^ (mixed >>> 27)) * 0x94d049bb133111ebL;
        return mixed ^ (mixed >>> 31);
    }
}
