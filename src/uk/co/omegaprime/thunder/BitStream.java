package uk.co.omegaprime.thunder;

import static uk.co.omegaprime.thunder.Bits.bigEndian;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class BitStream {
    private long ptr;
    private long endPtr;
    private byte bitOffset;

    public BitStream() {
        this(0, 0);
    }

    public BitStream(long ptr, int sz) {
        initialize(ptr, sz);
    }

    // NB: cannot use a returned mark once the BitStream has been reinitialized (and hence the endPtr changed)
    public long mark() {
        if ((ptr & 0xE000000000000000l) != 0l) {
            throw new IllegalStateException("This code is relying on the invariant that pointers on x64 are only 48 bits long and don't use their upper bits");
        }
        return ptr | ((long)bitOffset << 61);
    }

    public void reset(long mark) {
        bitOffset = (byte)(mark >>> 61);
        ptr = mark & ~0xE000000000000000l;
    }

    public void initialize(long ptr, int sz) {
        this.ptr = ptr;
        this.endPtr = ptr + sz;
        this.bitOffset = 0;
    }

    // Fill any remaining bits of this byte with zeros.
    // Particularly important to do this when writing keys or we won't be able to get the values we put!
    public void zeroFill() {
        if (bitOffset != 0) {
            unsafe.putByte(ptr, (byte)(unsafe.getByte(ptr) & (0xFF << (8 - bitOffset))));
        }
    }

    // NB: can't implement remainingBits since we only get an endPtr, not an endBitOffset
    public int remainingBytes() {
        return (int)(endPtr - ptr) - bitOffset == 0 ? 0 : 1;
    }

    public boolean getBoolean() {
        byte x = unsafe.getByte(ptr);
        boolean result = (((x << bitOffset) >> 7) & 1) == 1;
        advanceBits(1);
        return result;
    }

    public byte getByte() {
        short x = bigEndian(unsafe.getShort(ptr));
        byte result = (byte)((x << bitOffset) >> 8);
        advance(1);
        return result;
    }

    public int getInt() {
        long x = bigEndian(unsafe.getLong(ptr));
        int result = (int)((x << bitOffset) >> 32);
        advance(4);
        return result;
    }

    public long getLong() {
        long x0 = bigEndian(unsafe.getLong(ptr));
        long result0 = ((x0 << bitOffset) >>> 32);
        long x1 = bigEndian(unsafe.getLong(ptr + 4));
        long result1 = ((x1 << bitOffset) >>> 32);
        advance(8);
        return (result0 << 32) | result1;
    }

    public void putBoolean(boolean x) {
        byte current = unsafe.getByte(ptr);
        int mask = 1 << (7 - bitOffset);
        unsafe.putByte(ptr, (byte)(x ? current | mask : current & ~mask));
        advanceBits(1);
    }

    public void putByte(byte x) {
        final int mask = 0xFF << (8 - bitOffset);
        int cleared = bigEndian(unsafe.getShort(ptr)) & ~mask;
        unsafe.putShort(ptr, bigEndian((short)(cleared | (x << (8 - bitOffset)))));
        advance(1);
    }

    public void putInt(int x) {
        final long mask = 0xFFFFFFFFl << (32 - bitOffset);
        long cleared = bigEndian(unsafe.getLong(ptr)) & ~mask;
        unsafe.putLong(ptr, bigEndian(cleared | ((x & 0xFFFFFFFFl) << (32 - bitOffset))));
        advance(4);
    }

    public void putLong(long x) {
        // Fake it by doing two 32-bit writes:
        final long mask = 0xFFFFFFFFl << (32 - bitOffset);
        {
            long cleared = bigEndian(unsafe.getLong(ptr)) & ~mask;
            unsafe.putLong(ptr, bigEndian(cleared | ((x >>> 32) << (32 - bitOffset))));
        }
        {
            long cleared = bigEndian(unsafe.getLong(ptr + 4)) & ~mask;
            unsafe.putLong(ptr + 4, bigEndian(cleared | ((x & 0xFFFFFFFFl) << (32 - bitOffset))));
        }
        advance(8);
    }

    public void advance(int nBytes) {
        advanceBits(nBytes * 8);
    }

    public void advanceBits(int nBits) {
        int newBitOffset = bitOffset + nBits;
        ptr += newBitOffset / 8;
        bitOffset = (byte)(newBitOffset % 8);
    }
}
