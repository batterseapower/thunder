package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

public class Latin1StringSchema implements Schema<String> {
    public static Latin1StringSchema INSTANCE = new Latin1StringSchema();

    private final int maximumLength;

    public Latin1StringSchema() { this(-1); }
    public Latin1StringSchema(int maximumLength) { this.maximumLength = maximumLength; }

    @Override
    public String read(BitStream bs) {
        final long mark = bs.mark();
        int count = 0;
        while (bs.getBoolean()) {
            count++;
            bs.getByte();
        }
        bs.reset(mark);

        final char[] cs = new char[count];
        for (int i = 0; i < cs.length; i++) {
            if (!bs.getBoolean()) throw new IllegalStateException("Latin1StringSchema.read: impossible");
            cs[i] = (char)bs.getByte();
        }
        if (bs.getBoolean()) throw new IllegalStateException("Latin1StringSchema.read: impossible");
        return new String(cs);
    }

    @Override
    public int maximumSizeBits() {
        return maximumLength * 9 + 1;
    }

    @Override
    public int sizeBits(String x) {
        return x.length() * 9 + 1;
    }

    @Override
    public void write(BitStream bs, String x) {
        if (maximumLength >= 0 && x.length() > maximumLength) {
            throw new IllegalArgumentException("Supplied string " + x + " would be truncated to maximum size of " + maximumLength + " chars");
        }

        for (int i = 0; i < x.length(); i++) {
            final char c = x.charAt(i);
            bs.putBoolean(true);
            bs.putByte((byte)((int)c < 255 ? c : '?'));
        }
        bs.putBoolean(false);
    }

}
