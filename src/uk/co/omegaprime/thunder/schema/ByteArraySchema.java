package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class ByteArraySchema implements Schema<byte[]> {
    public static Schema<byte[]> INSTANCE = new ByteArraySchema();

    public byte[] read(BitStream bs) {
        final long mark = bs.mark();
        int count = 0;
        while (bs.getBoolean()) {
            count++;
            bs.getByte();
        }
        bs.reset(mark);

        final byte[] xs = new byte[count];
        for (int i = 0; i < xs.length; i++) {
            if (!bs.getBoolean()) throw new IllegalStateException("ByteArraySchema.read: impossible");
            xs[i] = bs.getByte();
        }
        if (bs.getBoolean()) throw new IllegalStateException("ByteArraySchema.read: impossible");
        return xs;
    }

    public int maximumSizeBits() { return -1; }
    public int sizeBits(byte[] x) { return x.length * 9 + 1; }

    public void write(BitStream bs, byte[] xs) {
        for (byte x : xs) {
            bs.putBoolean(true);
            bs.putByte(x);
        }
        bs.putBoolean(false);
    }
}
