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
}
