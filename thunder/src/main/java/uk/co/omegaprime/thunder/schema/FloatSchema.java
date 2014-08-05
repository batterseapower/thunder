package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class FloatSchema implements Schema<Float> {
    public static FloatSchema INSTANCE = new FloatSchema();

    // This sign-swapping magic is due to HBase's OrderedBytes class (and from Orderly before that)
    private int toDB(int l) {
        return l ^ ((l >> Integer.SIZE - 1) | Integer.MIN_VALUE);
    }

    private int fromDB(int l) {
        return l ^ ((~l >> Integer.SIZE - 1) | Integer.MIN_VALUE);
    }

    public Float read(BitStream bs) { return Float.intBitsToFloat(fromDB(bs.getInt())); }
    public int maximumSizeBits() { return Float.BYTES * 8; }
    public int sizeBits(Float x) { return maximumSizeBits(); }
    public void write(BitStream bs, Float x) { bs.putInt(toDB(Float.floatToRawIntBits(x))); }
}
