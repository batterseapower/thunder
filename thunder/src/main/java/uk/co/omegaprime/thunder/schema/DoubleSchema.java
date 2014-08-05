package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class DoubleSchema implements Schema<Double> {
    public static DoubleSchema INSTANCE = new DoubleSchema();

    // This sign-swapping magic is due to HBase's OrderedBytes class (and from Orderly before that)
    private long toDB(long l) {
        return l ^ ((l >> Long.SIZE - 1) | Long.MIN_VALUE);
    }

    private long fromDB(long l) {
        return l ^ ((~l >> Long.SIZE - 1) | Long.MIN_VALUE);
    }

    public Double read(BitStream bs) { return Double.longBitsToDouble(fromDB(bs.getLong())); }
    public int maximumSizeBits() { return Double.BYTES * 8; }
    public int sizeBits(Double x) { return maximumSizeBits(); }
    public void write(BitStream bs, Double x) { bs.putLong(toDB(Double.doubleToRawLongBits(x))); }
}
