package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class UntypedCursor implements AutoCloseable {
    protected final UntypedDatabase database;
    protected final Transaction tx;
    protected final long cursor;

    // Unlike the bufferPtrs in Database, it is important the the state of this var persists across calls:
    // it basically holds info about what the cursor is currently pointing to.
    //
    // If bufferPtrGeneration is different from the current transaction generation then the contents of this buffer
    // aren't actually guaranteed to be right, and you will have to call move(JNI.MDB_GET_CURRENT) to correct
    // this situation. We could avoid this ever happening in many cases by just call this eagerly after any
    // operation (e.g. a put) that leaves the bufferPtr stale, but because *other* cursors can invalidate
    // bufferPtr as a side effect of their own updates this is a actually bit tricky to guarantee.
    protected final long bufferPtr = unsafe.allocateMemory(4 * Unsafe.ADDRESS_SIZE);
    protected long bufferPtrGeneration;
    protected long references = 0;

    public UntypedCursor(UntypedDatabase database, Transaction tx, long cursor) {
        this.database = database;
        this.tx = tx;
        this.cursor = cursor;
        this.bufferPtrGeneration = tx.generation - 1;
    }

    protected boolean isFound(int rc) {
        // The EINVAL check is a bit dodgy. The issue is that LMDB reports EINVAL instead of MDB_NOTFOUND
        // if e.g. the cursor is not positioned and you do a move to MDB_CURRENT. This is important if we
        // expect our isPositioned() implementation to work.
        if (rc == JNI.MDB_NOTFOUND || rc == JNI.EINVAL) {
            return false;
        } else {
            Util.checkErrorCode(rc);
            return true;
        }
    }

    protected boolean move(int op) {
        boolean result = isFound(JNI.mdb_cursor_get(cursor, bufferPtr, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        bufferPtrGeneration = tx.generation;
        return result;
    }

    public boolean moveFirst()    { return move(JNI.MDB_FIRST); }
    public boolean moveLast()     { return move(JNI.MDB_LAST); }
    public boolean moveNext()     { return move(JNI.MDB_NEXT); }
    public boolean movePrevious() { return move(JNI.MDB_PREV); }
    public boolean isPositioned() { return move(JNI.MDB_GET_CURRENT); }

    protected boolean refreshBufferPtr() { return bufferPtrGeneration == tx.generation || move(JNI.MDB_GET_CURRENT); }

    <K> boolean move(BufferedSchema<K> kBuffer, K k, int op) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        try {
            return isFound(JNI.mdb_cursor_get(cursor, kBufferPtrNow, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        } finally {
            // Need to copy the MDB_val from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
            unsafe.putAddress(bufferPtr,                       unsafe.getAddress(kBufferPtrNow));
            unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
            bufferPtrGeneration = tx.generation;
            kBuffer.free(kBufferPtrNow);
        }
    }

    public <K> boolean moveTo(BufferedSchema<K> kBuffer, K k)      { return move(kBuffer, k, JNI.MDB_SET_KEY); }
    public <K> boolean moveCeiling(BufferedSchema<K> kBuffer, K k) { return move(kBuffer, k, JNI.MDB_SET_RANGE); }
    public <K> boolean moveFloor(BufferedSchema<K> kBuffer, K k) {
        return (moveCeiling(kBuffer, k) && keyEquals(kBuffer, k)) || movePrevious();
    }

    protected <K> boolean keyEquals(BufferedSchema<K> kBuffer, K k) {
        return keyValueEquals(k, 0, kBuffer, false);
    }

    protected <K> boolean keyStartsWith(BufferedSchema<K> kBuffer, K k) {
        return keyValueEquals(k, 0, kBuffer, true);
    }

    protected <V> boolean valueEquals(BufferedSchema<V> vBuffer, V v) {
        return keyValueEquals(v, 2 * Unsafe.ADDRESS_SIZE, vBuffer, false);
    }

    private <T> boolean keyValueEquals(T kv, int byteOffsetFromBufferPtr, BufferedSchema<T> buffer, boolean allowOurValueToBeAPrefix) {
        refreshBufferPtr();

        final int szBits = buffer.getSchema().sizeBits(kv);
        final int sz = bitsToBytes(szBits);

        final long theirSz = unsafe.getAddress(bufferPtr + byteOffsetFromBufferPtr);
        if (allowOurValueToBeAPrefix ? sz > theirSz : sz != theirSz) {
            return false;
        }

        final long bufferPtrNow = buffer.allocate(sz);
        buffer.write(bufferPtrNow, sz, kv);
        try {
            final long ourPtr   = unsafe.getAddress(bufferPtr + byteOffsetFromBufferPtr + Unsafe.ADDRESS_SIZE);
            final long theirPtr = bufferPtrNow + 2 * Unsafe.ADDRESS_SIZE;
            for (int i = 0; i < sz - (szBits % 8 != 0 ? 1 : 0); i++) {
                if (unsafe.getByte(ourPtr + i) != unsafe.getByte(theirPtr + i)) {
                    return false;
                }
            }

            if (szBits % 8 != 0) {
                // Say szBits % 8 == 1
                // ==> ~((1 << 8 - (szBits % 8)) - 1) == ~((1 << 7) - 1) == ~(10000000b - 1) == ~01111111b == 10000000b
                final int mask = ~((1 << 8 - (szBits % 8)) - 1);
                if ((unsafe.getByte(ourPtr + (szBits / 8)) & mask) != (unsafe.getByte(theirPtr + (szBits / 8)) & mask)) {
                    return false;
                }
            }

            return true;
        } finally {
            buffer.free(bufferPtrNow);
        }
    }

    public <K> K getKey(BufferedSchema<K> kBuffer) {
        refreshBufferPtr();
        return kBuffer.read(bufferPtr);
    }

    public <V> V getValue(BufferedSchema<V> vBuffer) {
        refreshBufferPtr();
        return vBuffer.read(bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
    }

    public <K, V> void put(BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, V v) {
        refreshBufferPtr();

        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        // You might think we could just reuse the existing key in bufferPtr (that we know to be correct).
        // Unfortunately we have to copy the key into a fresh buffer and give that to mdb_cursor_put instead.
        // Reason: the pointers in bufferPtr generally come from mdb_get, and as the docs state "Values returned
        // from the database are valid only until a subsequent update operation, or the end of the transaction".
        // In particular I found that trying to use them as an *input* to an update operation causes DB corruption.
        final long kBufferPtrNow = kBuffer.allocateAndCopy(bufferPtr);
        try {
            unsafe.putAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            vBuffer.writeDirect(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz, v);
        } finally {
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    // This method has a lot in common with Database.put. LMDB actually just implements mdb_put using mdb_cursor_put, so this makes sense!
    public <K, V> void put(BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
            vBuffer.writeDirect(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public <K, V> V putIfAbsent(BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            final int rc = JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE | JNI.MDB_NOOVERWRITE);
            if (rc == JNI.MDB_KEYEXIST) {
                return vBuffer.read(vBufferPtrNow);
            } else {
                Util.checkErrorCode(rc);
                vBuffer.write(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
                return null;
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public void delete() {
        Util.checkErrorCode(JNI.mdb_cursor_del(cursor, 0));
        tx.generation++;
    }

    public void close() {
        unsafe.freeMemory(bufferPtr);
        JNI.mdb_cursor_close(cursor);
    }
}
