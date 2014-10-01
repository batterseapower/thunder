package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

// XXX: type specialisation for true 0-allocation? But we might hope that escape analysis would save us because our boxes are intermediate only.
public class Cursor<K, V> implements Cursorlike<K,V>, AutoCloseable {
    final Index<K, V> index;
    final Transaction tx;
    final long cursor;

    // Unlike the bufferPtrs in Index, it is important the the state of this var persists across calls:
    // it basically holds info about what the cursor is currently pointing to.
    //
    // If bufferPtrGeneration is different from the current transaction generation then the contents of this buffer
    // aren't actually guaranteed to be right, and you will have to call move(JNI.MDB_GET_CURRENT) to correct
    // this situation. We could avoid this ever happening in many cases by just call this eagerly after any
    // operation (e.g. a put) that leaves the bufferPtr stale, but because *other* cursors can invalidate
    // bufferPtr as a side effect of their own updates this is a actually bit tricky to guarantee.
    static class Shared {
        final long bufferPtr = unsafe.allocateMemory(4 * Unsafe.ADDRESS_SIZE);
        long bufferPtrGeneration;
        long references = 0;

        public Shared(Transaction tx) {
            this.bufferPtrGeneration = tx.generation - 1;
        }
    }

    final Shared shared;

    public Cursor(Index<K, V> index, Transaction tx, long cursor) {
        this(index, tx, cursor, new Shared(tx));
    }

    private Cursor(Index<K, V> index, Transaction tx, long cursor, Shared shared) {
        this.index = index;
        this.tx = tx;
        this.cursor = cursor;
        this.shared = shared;

        shared.references++;
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
        boolean result = isFound(JNI.mdb_cursor_get(cursor, shared.bufferPtr, shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        shared.bufferPtrGeneration = tx.generation;
        return result;
    }

    @Override public boolean moveFirst()    { return move(JNI.MDB_FIRST); }
    @Override public boolean moveLast()     { return move(JNI.MDB_LAST); }
    @Override public boolean moveNext()     { return move(JNI.MDB_NEXT); }
    @Override public boolean movePrevious() { return move(JNI.MDB_PREV); }
    @Override public boolean isPositioned() { return move(JNI.MDB_GET_CURRENT); }

    protected boolean refreshBufferPtr() { return shared.bufferPtrGeneration == tx.generation || move(JNI.MDB_GET_CURRENT); }

    private boolean move(K k, int op) {
        final int kSz = bitsToBytes(index.kSchema.sizeBits(k));

        final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
        index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
        try {
            return isFound(JNI.mdb_cursor_get(cursor, kBufferPtrNow, shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        } finally {
            // Need to copy the MDB_val from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
            unsafe.putAddress(shared.bufferPtr,                       unsafe.getAddress(kBufferPtrNow));
            unsafe.putAddress(shared.bufferPtr + Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
            shared.bufferPtrGeneration = tx.generation;
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
        }
    }

    @Override public boolean moveTo(K k)      { return move(k, JNI.MDB_SET_KEY); }
    @Override public boolean moveCeiling(K k) { return move(k, JNI.MDB_SET_RANGE); }

    @Override public boolean moveFloor(K k) {
        return (moveCeiling(k) && keyEquals(k)) || movePrevious();
    }

    protected boolean keyEquals(K k) {
        return keyValueEquals(k, 0, index.kSchema, index.kBufferPtr, false);
    }

    protected boolean keyStartsWith(K k) {
        return keyValueEquals(k, 0, index.kSchema, index.kBufferPtr, true);
    }

    protected boolean valueEquals(V v) {
        return keyValueEquals(v, 2 * Unsafe.ADDRESS_SIZE, index.vSchema, index.vBufferPtr, false);
    }

    private <T> boolean keyValueEquals(T kv, int byteOffsetFromBufferPtr, Schema<T> schema, long scratchBufferPtr, boolean allowOurValueToBeAPrefix) {
        refreshBufferPtr();

        final int szBits = schema.sizeBits(kv);
        final int sz = bitsToBytes(szBits);

        final long theirSz = unsafe.getAddress(shared.bufferPtr + byteOffsetFromBufferPtr);
        if (allowOurValueToBeAPrefix ? sz > theirSz : sz != theirSz) {
            return false;
        }

        final long bufferPtrNow = Index.allocateBufferPointer(scratchBufferPtr, sz);
        index.fillBufferPointerFromSchema(schema, bufferPtrNow, sz, kv);
        try {
            final long ourPtr   = unsafe.getAddress(shared.bufferPtr + byteOffsetFromBufferPtr + Unsafe.ADDRESS_SIZE);
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
            Index.freeBufferPointer(scratchBufferPtr, bufferPtrNow);
        }
    }

    @Override
    public K getKey() {
        refreshBufferPtr();
        index.bs.initialize(unsafe.getAddress(shared.bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(shared.bufferPtr));
        return index.kSchema.read(index.bs);
    }

    @Override
    public V getValue() {
        refreshBufferPtr();
        index.bs.initialize(unsafe.getAddress(shared.bufferPtr + 3 * Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE));
        return index.vSchema.read(index.bs);
    }

    @Override
    public void put(V v) {
        refreshBufferPtr();

        final int vSz = bitsToBytes(index.vSchema.sizeBits(v));

        // You might think we could just reuse the existing key in bufferPtr (that we know to be correct).
        // Unfortunately we have to copy the key into a fresh buffer and give that to mdb_cursor_put instead.
        // Reason: the pointers in bufferPtr generally come from mdb_get, and as the docs state "Values returned
        // from the database are valid only until a subsequent update operation, or the end of the transaction".
        // In particular I found that trying to use them as an *input* to an update operation causes DB corruption.
        final long kBufferPtrNow = Index.allocateAndCopyBufferPointer(index.kBufferPtr, shared.bufferPtr);
        try {
            unsafe.putAddress(shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            index.bs.initialize(unsafe.getAddress(shared.bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz);
            index.vSchema.write(index.bs, v);
            index.bs.zeroFill();
        } finally {
            Index.freeBufferPointer(index.kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    // This method has a lot in common with Index.put. LMDB actually just implements mdb_put using mdb_cursor_put, so this makes sense!
    @Override
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
            tx.generation++;
        }
    }

    @Override
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
            tx.generation++;
        }
    }

    @Override
    public void delete() {
        Util.checkErrorCode(JNI.mdb_cursor_del(cursor, 0));
        tx.generation++;
    }

    public void close() {
        if (--shared.references == 0) {
            unsafe.freeMemory(shared.bufferPtr);
            JNI.mdb_cursor_close(cursor);
        }
    }

    public <K2, V2> Cursor<K2, V2> reinterpretView(Schema<K2> k2Schema, Schema<V2> v2Schema) {
        // It is because both this method and close exist that we need to ref count the buffer:
        return new Cursor<>(index.reinterpretView(k2Schema, v2Schema), tx, cursor, shared);
    }

    @Override public Schema<K> getKeySchema()   { return index.getKeySchema(); }
    @Override public Schema<V> getValueSchema() { return index.getValueSchema(); }

    @Override
    public Index<K, V> getIndex() { return index; }
}
