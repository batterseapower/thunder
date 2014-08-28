package uk.co.omegaprime.thunder;

import org.junit.Test;

import static org.junit.Assert.*;

public class BitStreamTest {
    @Test
    public void muchTestWow() {
        final long ptr = Bits.unsafe.allocateMemory(16);

        {
            final BitStream bs = new BitStream(ptr, 16);
            bs.putByte((byte)1);
            bs.putInt(1337);
            bs.putBoolean(false);
            bs.putInt(128);
            bs.putByte((byte)7);
            bs.putBoolean(true);
            bs.putInt(42);
            bs.putLong(100);
        }

        {
            final BitStream bs = new BitStream(ptr, 16);
            assertEquals(1, bs.getByte());
            assertEquals(1337, bs.getInt());
            assertFalse(bs.getBoolean());
            assertEquals(128, bs.getInt());
            assertEquals(7, bs.getByte());
            assertTrue(bs.getBoolean());
            assertEquals(42, bs.getInt());
            assertEquals(100, bs.getLong());
        }
    }

    @Test
    public void testLongs() {
        final long ptr = Bits.unsafe.allocateMemory(33);

        {
            final BitStream bs = new BitStream(ptr, 33);
            bs.putLong(1337);
            bs.putLong(-1337);
            bs.putLong(100);
            bs.putLong(-100);
        }

        {
            final BitStream bs = new BitStream(ptr, 33);
            assertEquals(1337, bs.getLong());
            assertEquals(-1337, bs.getLong());
            assertEquals(100, bs.getLong());
            assertEquals(-100, bs.getLong());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void testIncrementBitStreamFromMarkByteBoundaryNoOverflow() {
        final long ptr = Bits.unsafe.allocateMemory(32);

        {
            final BitStream bs = new BitStream(ptr, 32);
            bs.putInt(100);
            final long mark0 = bs.mark();
            bs.putInt(200);
            bs.putInt(300);
            assertFalse(bs.incrementBitStreamFromMark(mark0));
            final long mark1 = bs.mark();
            bs.putInt(0xFFF);
            assertFalse(bs.incrementBitStreamFromMark(mark1));
        }

        {
            final BitStream bs = new BitStream(ptr, 32);
            assertEquals(100, bs.getInt());
            assertEquals(200, bs.getInt());
            assertEquals(301, bs.getInt());
            assertEquals(0x1000, bs.getInt());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void testIncrementBitStreamFromMarkByteBoundaryOverflow() {
        final long ptr = Bits.unsafe.allocateMemory(32);

        {
            final BitStream bs = new BitStream(ptr, 32);
            bs.putInt(100);
            final long mark = bs.mark();
            bs.putInt(0xFFFFFFFF);
            bs.putInt(0xFFFFFFFF);
            assertTrue(bs.incrementBitStreamFromMark(mark));
        }

        {
            final BitStream bs = new BitStream(ptr, 32);
            assertEquals(100, bs.getInt());
            assertEquals(0, bs.getInt());
            assertEquals(0, bs.getInt());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void testIncrementBitStreamFromMarkSubByteBits() {
        final long ptr = Bits.unsafe.allocateMemory(32);

        {
            final BitStream bs = new BitStream(ptr, 32);
            bs.putBoolean(true);
            final long mark0 = bs.mark();
            bs.putBoolean(false);
            bs.putBoolean(false);
            bs.putBoolean(true);
            assertFalse(bs.incrementBitStreamFromMark(mark0));
            final long mark1 = bs.mark();
            bs.putBoolean(true);
            bs.putBoolean(true);
            bs.putBoolean(true);
            assertTrue(bs.incrementBitStreamFromMark(mark1));
        }

        {
            final BitStream bs = new BitStream(ptr, 32);
            assertTrue(bs.getBoolean());
            assertFalse(bs.getBoolean());
            assertTrue(bs.getBoolean());
            assertFalse(bs.getBoolean());
            assertFalse(bs.getBoolean());
            assertFalse(bs.getBoolean());
            assertFalse(bs.getBoolean());
        }

        Bits.unsafe.freeMemory(ptr);
    }

    @Test
    public void testIncrementBitStreamFromMarkMultiByteBits() {
        final long ptr = Bits.unsafe.allocateMemory(32);

        {
            final BitStream bs = new BitStream(ptr, 32);
            bs.putBoolean(true);
            final long mark0 = bs.mark();
            bs.putInt(100);
            assertFalse(bs.incrementBitStreamFromMark(mark0));
            final long mark1 = bs.mark();
            bs.putInt(0xFFFFFFFF);
            assertTrue(bs.incrementBitStreamFromMark(mark1));
        }

        {
            final BitStream bs = new BitStream(ptr, 32);
            assertTrue(bs.getBoolean());
            assertEquals(101, bs.getInt());
            assertEquals(0, bs.getInt());
        }

        Bits.unsafe.freeMemory(ptr);
    }
}
