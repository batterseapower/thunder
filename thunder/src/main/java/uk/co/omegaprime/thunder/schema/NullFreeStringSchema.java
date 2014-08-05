package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

import java.nio.charset.Charset;

// XXX: should this be the default?
public class NullFreeStringSchema implements Schema<String> {
    public static NullFreeStringSchema INSTANCE = new NullFreeStringSchema();

    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public String read(BitStream bs) {
        final long mark = bs.mark();
        int count = 0;
        do {
            byte b = bs.getByte();
            if (b == 0) {
                break;
            } else if ((b & 0xF0) == 0xF0) {
                bs.getByte();
                bs.getByte();
                bs.getByte();
            } else if ((b & 0xE0) == 0xE0) {
                bs.getByte();
                bs.getByte();
            } else if ((b & 0xC0) == 0xC0) {
                bs.getByte();
            }
            count++;
        } while (true);
        bs.reset(mark);

        final byte[] bytes = new byte[count];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = bs.getByte();
        }
        if (bs.getByte() != 0) throw new IllegalStateException("NullFreeStringSchema.read(): impossible");

        return new String(bytes, UTF8);
    }

    @Override
    public int maximumSizeBits() {
        return -1;
    }

    @Override
    public int sizeBits(String x) {
        return x.getBytes(UTF8).length * 8 + 8;
    }

    @Override
    public void write(BitStream bs, String x) {
        final byte[] bytes = x.getBytes(UTF8);
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0) {
                throw new IllegalArgumentException("Input string " + x + " contained a null byte");
            }
            bs.putByte(bytes[i]);
        }
        bs.putByte((byte)0);
    }
}
