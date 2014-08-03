package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class UnsignedLongSchema implements Schema<Long> {
    public static UnsignedLongSchema INSTANCE = new UnsignedLongSchema();

    public Long read(BitStream bs) { return bs.getLong(); }
    public int maximumSizeBits() { return Long.BYTES * 8; }
    public int sizeBits(Long x) { return maximumSizeBits(); }
    public void write(BitStream bs, Long x) { bs.putLong(x); }
}
