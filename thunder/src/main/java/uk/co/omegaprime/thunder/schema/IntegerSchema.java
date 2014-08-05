package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

import static uk.co.omegaprime.thunder.Bits.swapSign;

public class IntegerSchema implements Schema<Integer> {
    public static IntegerSchema INSTANCE = new IntegerSchema();

    public Integer read(BitStream bs) { return swapSign(bs.getInt()); }
    public int maximumSizeBits() { return Integer.BYTES * 8; }
    public int sizeBits(Integer x) { return maximumSizeBits(); }
    public void write(BitStream bs, Integer x) { bs.putInt(swapSign(x)); }
}
