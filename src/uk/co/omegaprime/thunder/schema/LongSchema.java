package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

import static uk.co.omegaprime.thunder.Bits.swapSign;

public class LongSchema implements Schema<Long> {
    public static LongSchema INSTANCE = new LongSchema();

    public Long read(BitStream bs) { return swapSign(bs.getLong()); }
    public int maximumSizeBits() { return Long.BYTES * 8; }
    public int sizeBits(Long x) { return maximumSizeBits(); }
    public void write(BitStream bs, Long x) { bs.putLong(swapSign(x)); }
}
