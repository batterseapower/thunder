package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class VoidSchema implements Schema<Void> {
    public static VoidSchema INSTANCE = new VoidSchema();

    public Void read(BitStream bs) { return null; }
    public int maximumSizeBits() { return 0; }
    public int sizeBits(Void x) { return maximumSizeBits(); }
    public void write(BitStream bs, Void x) { }
}
