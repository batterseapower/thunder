package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class Index<K, V> implements AutoCloseable {
    final Database db;
    final long dbi;
    final Schema<K> kSchema;
    final Schema<V> vSchema;

    // Used for temporary scratch storage within the context of a single method only, basically
    // just to save some calls to the allocator. The sole reason why Index is not thread safe.
    final long kBufferPtr, vBufferPtr;
    final BitStream bs = new BitStream();

    public Index(Database db, long dbi, Schema<K> kSchema, Schema<V> vSchema) {
        this.db = db;
        this.dbi = dbi;
        this.kSchema = kSchema;
        this.vSchema = vSchema;

        this.kBufferPtr = allocateSharedBufferPointer(kSchema);
        this.vBufferPtr = allocateSharedBufferPointer(vSchema);
    }

    public Schema<K> getKeySchema()   { return kSchema; }
    public Schema<V> getValueSchema() { return vSchema; }

    private static <T> long allocateSharedBufferPointer(Schema<T> schema) {
        if (schema.maximumSizeBits() < 0) {
            // TODO: speculatively allocate a reasonable amount of memory that most allocations of interest might fit into?
            return 0;
        } else {
            return allocateBufferPointer(0, bitsToBytes(schema.maximumSizeBits()));
        }
    }

    private static void freeSharedBufferPointer(long bufferPtr) {
        if (bufferPtr != 0) {
            unsafe.freeMemory(bufferPtr);
        }
    }

    // INVARIANT: sz == schema.size(x)
    protected <T> void fillBufferPointerFromSchema(Schema<T> schema, long bufferPtr, int sz, T x) {
        unsafe.putAddress(bufferPtr, sz);
        unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
        bs.initialize(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz);
        schema.write(bs, x);
        bs.zeroFill();
    }

    protected static long allocateBufferPointer(long bufferPtr, int sz) {
        if (bufferPtr != 0) {
            return bufferPtr;
        } else {
            return unsafe.allocateMemory(2 * Unsafe.ADDRESS_SIZE + sz);
        }
    }

    protected static long allocateAndCopyBufferPointer(long bufferPtr, long bufferPtrToCopy) {
        int sz = (int)unsafe.getAddress(bufferPtrToCopy);
        long bufferPtrNow = Index.allocateBufferPointer(bufferPtr, sz);
        unsafe.putAddress(bufferPtrNow,                       sz);
        unsafe.putAddress(bufferPtrNow + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
        unsafe.copyMemory(unsafe.getAddress(bufferPtrToCopy + Unsafe.ADDRESS_SIZE), bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz);
        return bufferPtrNow;
    }

    protected static void freeBufferPointer(long bufferPtr, long bufferPtrNow) {
        if (bufferPtr == 0) {
            unsafe.freeMemory(bufferPtrNow);
        }
    }

    public Cursor<K, V> createCursor(Transaction tx) {
        final long[] cursorPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
        return new Cursor<>(this, tx, cursorPtr[0]);
    }

    public void close() {
        freeSharedBufferPointer(kBufferPtr);
        freeSharedBufferPointer(vBufferPtr);
        JNI.mdb_dbi_close(db.env, dbi);
    }

    public void put(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            Util.checkErrorCode(JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
            assert(unsafe.getAddress(vBufferPtrNow) == vSz);
            bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
            vSchema.write(bs, v);
            bs.zeroFill();
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    public V putIfAbsent(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
        fillBufferPointerFromSchema(vSchema, vBufferPtrNow, vSz, v);
        try {
            int rc = JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE | JNI.MDB_NOOVERWRITE);
            if (rc == JNI.MDB_KEYEXIST) {
                bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(vBufferPtrNow));
                return vSchema.read(bs);
            } else {
                Util.checkErrorCode(rc);
                assert(unsafe.getAddress(vBufferPtrNow) == vSz);
                bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
                vSchema.write(bs, v);
                bs.zeroFill();
                return null;
            }
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    public boolean remove(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        try {
            int rc = JNI.mdb_del(tx.txn, dbi, kBufferPtrNow, 0);
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        } finally {
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    public V get(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, 0);
        try {
            int rc = JNI.mdb_get(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return null;
            } else {
                Util.checkErrorCode(rc);
                bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), (int) unsafe.getAddress(vBufferPtrNow));
                return vSchema.read(bs);
            }
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
        }
    }

    public boolean contains(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, 0);
        try {
            int rc = JNI.mdb_get(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
        }
    }

    public Iterator<K> keys(Transaction tx) {
        final Cursor<K, V> cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<K>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public K next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final K key = cursor.getKey();
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return key;
            }
        };
    }

    public Iterator<V> values(Transaction tx) {
        final Cursor<K, V> cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<V>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public V next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final V value = cursor.getValue();
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return value;
            }
        };
    }

    public Iterator<Pair<K, V>> keyValues(Transaction tx) {
        final Cursor<K, V> cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<Pair<K, V>>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Pair<K, V> next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final Pair<K, V> pair = new Pair<>(cursor.getKey(), cursor.getValue());
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return pair;
            }
        };
    }

    public <K2, V2> Index<K2, V2> reinterpretView(Schema<K2> k2Schema, Schema<V2> v2Schema) {
        // FIXME: ref count? Or ensure that returned thing can't be closed.
        return new Index<>(db, dbi, k2Schema, v2Schema);
    }
}
