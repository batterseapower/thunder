package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

// FIXME: bufferPtr may be invalidated by an operation on *another* cursor/index within the same transaction

// XXX: type specialisation for true 0-allocation? But we might hope that escape analysis would save us because our boxes are intermediate only.
public class Cursor<K, V> implements AutoCloseable {
    final Index<K, V> index;
    final long cursor;

    // Unlike the bufferPtrs in Index, it is important the the state of this var persists across calls:
    // it basically holds info about what the cursor is currently pointing to.
    //
    // If bufferPtrStale is true then the contents of this buffer aren't actually right, and you
    // will have to call move(JNI.MDB_GET_CURRENT) to correct this situation. An alternative to
    // having the bufferPtrStale flag would be to just call this eagerly whenever the buffer goes
    // stale, but I kind of like the idea of avoiding the JNI call (though TBH it doesn't seem to matter)
    final long bufferPtr;
    boolean bufferPtrStale;

    public Cursor(Index<K, V> index, long cursor) {
        this.index = index;
        this.cursor = cursor;

        this.bufferPtr = unsafe.allocateMemory(4 * Unsafe.ADDRESS_SIZE);
    }

    protected boolean isFound(int rc) {
        if (rc == JNI.MDB_NOTFOUND) {
            return false;
        } else {
            Util.checkErrorCode(rc);
            return true;
        }
    }

    protected boolean move(int op) {
        boolean result = isFound(JNI.mdb_cursor_get(cursor, bufferPtr, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        bufferPtrStale = false;
        return result;
    }

    public boolean moveFirst()    { return move(JNI.MDB_FIRST); }
    public boolean moveLast()     { return move(JNI.MDB_LAST); }
    public boolean moveNext()     { return move(JNI.MDB_NEXT); }
    public boolean movePrevious() { return move(JNI.MDB_PREV); }

    protected boolean refresh() { return move(JNI.MDB_GET_CURRENT); }

    private boolean move(K k, int op) {
        final int kSz = bitsToBytes(index.kSchema.sizeBits(k));

        final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
        index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
        try {
            return isFound(JNI.mdb_cursor_get(cursor, kBufferPtrNow, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        } finally {
            // Need to copy the MDB_val from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
            unsafe.putAddress(bufferPtr,                       unsafe.getAddress(kBufferPtrNow));
            unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
            bufferPtrStale = false;
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
        }
    }

    public boolean moveTo(K k)      { return move(k, JNI.MDB_SET_KEY); }
    public boolean moveCeiling(K k) { return move(k, JNI.MDB_SET_RANGE); }

    public boolean moveFloor(K k) {
        return (moveCeiling(k) && keyEquals(k)) || movePrevious();
    }

    protected boolean keyEquals(K k) {
        return keyValueEquals(k, 0, index.kSchema, index.kBufferPtr);
    }

    protected boolean valueEquals(V v) {
        return keyValueEquals(v, 2 * Unsafe.ADDRESS_SIZE, index.vSchema, index.vBufferPtr);
    }

    private <T> boolean keyValueEquals(T kv, int byteOffsetFromBufferPtr, Schema<T> schema, long scratchBufferPtr) {
        if (bufferPtrStale) { refresh(); }

        final int sz = bitsToBytes(schema.sizeBits(kv));
        if (sz != unsafe.getAddress(bufferPtr + byteOffsetFromBufferPtr)) {
            return false;
        }

        final long bufferPtrNow = Index.allocateBufferPointer(scratchBufferPtr, sz);
        index.fillBufferPointerFromSchema(schema, bufferPtrNow, sz, kv);
        try {
            final long ourPtr   = unsafe.getAddress(bufferPtr + byteOffsetFromBufferPtr + Unsafe.ADDRESS_SIZE);
            final long theirPtr = bufferPtrNow + 2 * Unsafe.ADDRESS_SIZE;
            for (int i = 0; i < sz; i++) {
                if (unsafe.getByte(ourPtr + i) != unsafe.getByte(theirPtr + i)) {
                    return false;
                }
            }

            return true;
        } finally {
            Index.freeBufferPointer(scratchBufferPtr, bufferPtrNow);
        }
    }

    public K getKey() {
        if (bufferPtrStale) { refresh(); }
        index.bs.initialize(unsafe.getAddress(bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr));
        return index.kSchema.read(index.bs);
    }

    public V getValue() {
        if (bufferPtrStale) { refresh(); }
        index.bs.initialize(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE));
        return index.vSchema.read(index.bs);
    }

    public void put(V v) {
        if (bufferPtrStale) { refresh(); }

        final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

        // You might think we could just reuse the existing key in bufferPtr (that we know to be correct).
        // Unfortunately we have to copy the key into a fresh buffer and give that to mdb_cursor_put instead.
        // Reason: the pointers in bufferPtr generally come from mdb_get, and as the docs state "Values returned
        // from the database are valid only until a subsequent update operation, or the end of the transaction".
        // In particular I found that trying to use them as an *input* to an update operation causes DB corruption.
        final long kBufferPtrNow = Index.allocateAndCopyBufferPointer(index.kBufferPtr, bufferPtr);
        try {
            unsafe.putAddress(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            index.bs.initialize(unsafe.getAddress(bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz);
            index.vSchema.write(index.bs, v);
            index.bs.zeroFill();
        } finally {
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            bufferPtrStale = true;
        }
    }

    // This method has a lot in common with Index.put. LMDB actually just implements mdb_put using mdb_cursor_put, so this makes sense!
    public void put(K k, V v) {
        final int kSz = bitsToBytes(index.kSchema.sizeBits(k));
        final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

        final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
        index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = Index.allocateBufferPointer(index.vBufferPtr, vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
            index.bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
            index.vSchema.write(index.bs, v);
            index.bs.zeroFill();
        } finally {
            Index.freeBufferPointer(index.vBufferPtr, vBufferPtrNow);
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            bufferPtrStale = true;
        }
    }

    public V putIfAbsent(K k, V v) {
        final int kSz = bitsToBytes(index.kSchema.sizeBits(k));
        final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

        final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
        index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = Index.allocateBufferPointer(index.vBufferPtr, vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            final int rc = JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE | JNI.MDB_NOOVERWRITE);
            if (rc == JNI.MDB_KEYEXIST) {
                index.bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(vBufferPtrNow));
                return index.vSchema.read(index.bs);
            } else {
                Util.checkErrorCode(rc);
                index.bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
                index.vSchema.write(index.bs, v);
                index.bs.zeroFill();
                return null;
            }
        } finally {
            Index.freeBufferPointer(index.vBufferPtr, vBufferPtrNow);
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            bufferPtrStale = true;
        }
    }

    public void delete() {
        Util.checkErrorCode(JNI.mdb_cursor_del(cursor, 0));
        bufferPtrStale = true;
    }

    public void close() {
        unsafe.freeMemory(bufferPtr);
        JNI.mdb_cursor_close(cursor);
    }
}
