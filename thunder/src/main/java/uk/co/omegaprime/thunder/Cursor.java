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
    static class Buffer {
        // XXX: could inline this back into Cursor. I used to share it between Cursor instances but decided against it.
        final long bufferPtr = unsafe.allocateMemory(4 * Unsafe.ADDRESS_SIZE);
        long bufferPtrGeneration;

        public Buffer(Transaction tx) {
            this.bufferPtrGeneration = tx.generation - 1;
        }
    }

    final Buffer buffer;

    public Cursor(Index<K, V> index, Transaction tx, long cursor) {
        this(index, tx, cursor, new Buffer(tx));
    }

    private Cursor(Index<K, V> index, Transaction tx, long cursor, Buffer buffer) {
        this.index = index;
        this.tx = tx;
        this.cursor = cursor;
        this.buffer = buffer;
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
        boolean result = isFound(JNI.mdb_cursor_get(cursor, buffer.bufferPtr, buffer.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        buffer.bufferPtrGeneration = tx.generation;
        return result;
    }

    @Override public boolean moveFirst()    { return move(JNI.MDB_FIRST); }
    @Override public boolean moveLast()     { return move(JNI.MDB_LAST); }
    @Override public boolean moveNext()     { return move(JNI.MDB_NEXT); }
    @Override public boolean movePrevious() { return move(JNI.MDB_PREV); }

    protected boolean refreshBufferPtr() { return buffer.bufferPtrGeneration == tx.generation || move(JNI.MDB_GET_CURRENT); }

    private boolean move(K k, int op) {
        final int kSz = bitsToBytes(index.kSchema.sizeBits(k));

        final long kBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, kSz);
        index.fillBufferPointerFromSchema(index.kSchema, kBufferPtrNow, kSz, k);
        try {
            return isFound(JNI.mdb_cursor_get(cursor, kBufferPtrNow, buffer.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, op));
        } finally {
            // Need to copy the MDB_val from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
            unsafe.putAddress(buffer.bufferPtr,                       unsafe.getAddress(kBufferPtrNow));
            unsafe.putAddress(buffer.bufferPtr + Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
            buffer.bufferPtrGeneration = tx.generation;
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

        final long theirSz = unsafe.getAddress(buffer.bufferPtr + byteOffsetFromBufferPtr);
        if (allowOurValueToBeAPrefix ? sz > theirSz : sz != theirSz) {
            return false;
        }

        final long bufferPtrNow = Index.allocateBufferPointer(scratchBufferPtr, sz);
        index.fillBufferPointerFromSchema(schema, bufferPtrNow, sz, kv);
        try {
            final long ourPtr   = unsafe.getAddress(buffer.bufferPtr + byteOffsetFromBufferPtr + Unsafe.ADDRESS_SIZE);
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
                if ((unsafe.getByte(ourPtr + (szBits / 8)) & mask) != (unsafe.getByte(ourPtr + (szBits / 8)) & mask)) {
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
        index.bs.initialize(unsafe.getAddress(buffer.bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(buffer.bufferPtr));
        return index.kSchema.read(index.bs);
    }

    @Override
    public V getValue() {
        refreshBufferPtr();
        index.bs.initialize(unsafe.getAddress(buffer.bufferPtr + 3 * Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(buffer.bufferPtr + 2 * Unsafe.ADDRESS_SIZE));
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
        final long kBufferPtrNow = Index.allocateAndCopyBufferPointer(index.kBufferPtr, buffer.bufferPtr);
        try {
            unsafe.putAddress(buffer.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, vSz);
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, buffer.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, JNI.MDB_CURRENT | JNI.MDB_RESERVE));
            index.bs.initialize(unsafe.getAddress(buffer.bufferPtr + 3 * Unsafe.ADDRESS_SIZE), vSz);
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
        unsafe.freeMemory(buffer.bufferPtr);
        JNI.mdb_cursor_close(cursor);
    }

    public <K2, V2> Cursor<K2, V2> reinterpretView(Schema<K2> k2Schema, Schema<V2> v2Schema) {
        // FIXME: ref count? Or ensure that returned thing can't be closed.
        return new Cursor<K2, V2>(index.reinterpretView(k2Schema, v2Schema), tx, cursor, buffer);
    }

    public Schema<K> getKeySchema()   { return index.getKeySchema(); }
    public Schema<V> getValueSchema() { return index.getValueSchema(); }

    // FIXME: only allow this to be used if the current key schema is a pair schema?
    // FIXME: version for cursors with duplicate keys?
    public <A, B> Cursorlike<B, V> subcursorView(Schema<A> aSchema, Schema<B> bSchema, final A a) {
        final Schema<A> aSuccSchema = new Schema<A>() {
            @Override
            public A read(BitStream bs) {
                throw new IllegalStateException("aSuccSchema.read");
            }

            @Override
            public int maximumSizeBits() {
                return aSchema.maximumSizeBits();
            }

            @Override
            public int sizeBits(A a) {
                return aSchema.sizeBits(a);
            }

            @Override
            public void write(BitStream bs, A a) {
                long mark = bs.mark();
                aSchema.write(bs, a);
                bs.incrementBitStreamFromMark(mark);
            }
        };

        // Bit of a hack here unfortunately...
        final boolean aIsMaximum;
        {
            final int aSz = bitsToBytes(aSchema.sizeBits(a));
            final long aBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, aSz);
            try {
                index.bs.initialize(aBufferPtrNow + 2 * Unsafe.ADDRESS_SIZE, aSz);
                final long mark = index.bs.mark();
                aSchema.write(index.bs, a);
                aIsMaximum = index.bs.incrementBitStreamFromMark(mark);
            } finally {
                Index.freeBufferPointer(index.kBufferPtr, aBufferPtrNow);
            }
        }

        // NB: it is for this purpose that seekView.keyEquals carefully does *not* compare the trailing bits of the
        // key/value as well -- for aSeekView those trailing bits would be the first bits of "b" so may be non-zero
        final Cursor<A, V>          aSeekView     = this.reinterpretView(aSchema, getValueSchema());
        final Cursor<A, V>          aSuccSeekView = this.reinterpretView(aSuccSchema, getValueSchema());
        final Cursor<Pair<A, B>, V> abSeekView    = this.reinterpretView(Schema.zip(aSchema, bSchema), getValueSchema());
        return new Cursorlike<B, V>() {
            @Override
            public boolean moveFirst() {
                return aSeekView.moveCeiling(a) && aSeekView.keyStartsWith(a);
            }

            @Override
            public boolean moveLast() {
                return (!aIsMaximum && aSuccSeekView.moveCeiling(a) ? aSeekView.movePrevious() : aSeekView.moveLast()) && aSeekView.keyStartsWith(a);
            }

            @Override
            public boolean moveNext() {
                return aSeekView.moveNext() && aSeekView.keyStartsWith(a);
            }

            @Override
            public boolean movePrevious() {
                return aSeekView.movePrevious() && aSeekView.keyStartsWith(a);
            }

            @Override
            public boolean moveTo(B b) {
                return abSeekView.moveTo(new Pair<>(a, b));
            }

            @Override
            public boolean moveCeiling(B b) {
                return abSeekView.moveCeiling(new Pair<>(a, b)) && aSeekView.keyStartsWith(a);
            }

            @Override
            public boolean moveFloor(B b) {
                return abSeekView.moveFloor(new Pair<>(a, b)) && aSeekView.keyStartsWith(a);
            }

            @Override
            public B getKey() {
                return abSeekView.getKey().v;
            }

            @Override
            public V getValue() {
                return abSeekView.getValue();
            }

            @Override
            public void put(V v) {
                abSeekView.put(v);
            }

            @Override
            public void put(B b, V v) {
                abSeekView.put(new Pair<>(a, b), v);
            }

            @Override
            public V putIfAbsent(B b, V v) {
                return abSeekView.putIfAbsent(new Pair<>(a, b), v);
            }

            @Override
            public void delete() {
                abSeekView.delete();
            }

            public <C, D> Cursorlike<D, V> subcursorView(Schema<C> cSchema, Schema<D> dSchema, final C c) {
                // B == C + D
                // K == A + B == A + C + D
                return Cursor.this.<Pair<A, C>, D>subcursorView(Schema.zip(aSchema, cSchema), dSchema, new Pair<A, C>(a, c));
            }
        };
    }
}
