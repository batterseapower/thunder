package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class UnsignedIntegerSchema implements Schema<Integer> {
    public static UnsignedIntegerSchema INSTANCE = new UnsignedIntegerSchema();

    public Integer read(BitStream bs) { return bs.getInt(); }
    public int maximumSizeBits() { return Integer.BYTES * 8; }
    public int sizeBits(Integer x) { return maximumSizeBits(); }
    public void write(BitStream bs, Integer x) { bs.putInt(x); }
}
