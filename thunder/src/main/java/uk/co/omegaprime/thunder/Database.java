package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class Database<K, V> {
    final Environment db;
    final long dbi;
    final Schema<K> kSchema;
    final Schema<V> vSchema;

    // Used for temporary scratch storage within the context of a single method only, basically
    // just to save some calls to the allocator. The sole reason why Database is not thread safe.
    final SharedBuffer kBuffer, vBuffer;
    final BitStream bs = new BitStream();

    public Database(Environment db, long dbi, Schema<K> kSchema, Schema<V> vSchema) {
        this.db = db;
        this.dbi = dbi; // NB: we never mdb_dbi_close. This should be safe, and avoids Database having to be AutoCloseable
        this.kSchema = kSchema;
        this.vSchema = vSchema;

        this.kBuffer = new SharedBuffer(kSchema);
        this.vBuffer = new SharedBuffer(vSchema);
    }

    public Schema<K> getKeySchema()   { return kSchema; }
    public Schema<V> getValueSchema() { return vSchema; }

    // INVARIANT: sz == schema.size(x)
    protected <T> void fillBufferPointerFromSchema(Schema<T> schema, long bufferPtr, int sz, T x) {
        unsafe.putAddress(bufferPtr, sz);
        unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
        bs.initialize(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz);
        schema.write(bs, x);
        bs.zeroFill();
    }

    public Cursor<K, V> createCursor(Transaction tx) {
        final long[] cursorPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
        return new Cursor<>(this, tx, cursorPtr[0]);
    }

    @Override
    public void finalize() throws Throwable {
        kBuffer.close();
        vBuffer.close();
        super.finalize();
    }

    public void put(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            Util.checkErrorCode(JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
            assert(unsafe.getAddress(vBufferPtrNow) == vSz);
            bs.initialize(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz);
            vSchema.write(bs, v);
            bs.zeroFill();
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public V putIfAbsent(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
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
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public boolean remove(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
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
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public V get(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(0);
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
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
        }
    }

    public boolean contains(Transaction tx, K k) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(0);
        try {
            int rc = JNI.mdb_get(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
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

    public <K2, V2> Database<K2, V2> reinterpretView(Schema<K2> k2Schema, Schema<V2> v2Schema) {
        return new Database<>(db, dbi, k2Schema, v2Schema);
    }
}
