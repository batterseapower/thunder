package uk.co.omegaprime.thunder;

import uk.co.omegaprime.thunder.schema.Schema;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;

public class DatabaseWithDuplicateKeys<K, V> extends Database<K, V> {
    public DatabaseWithDuplicateKeys(Environment db, long dbi, Schema<K> kSchema, Schema<V> vSchema) {
        super(db, dbi, kSchema, vSchema);
    }

    public CursorWithDuplicateKeys<K, V> createCursor(Transaction tx) {
        final long[] cursorPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
        return new CursorWithDuplicateKeys<>(this, tx, cursorPtr[0]);
    }

    // Override the base class because MDB_RESERVE doesn't really make sense with MDB_DUPSORT
    @Override
    public void put(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
        fillBufferPointerFromSchema(vSchema, vBufferPtrNow, vSz, v);
        try {
            Util.checkErrorCode(JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, 0));
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    // Override because MDB_RESERVE doesn't work, and need to use MDB_NODUPDATA rather than MDB_NOOVERWRITE
    @Override
    public V putIfAbsent(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
        fillBufferPointerFromSchema(vSchema, vBufferPtrNow, vSz, v);
        try {
            final int rc = JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_NODUPDATA);
            if (rc == JNI.MDB_KEYEXIST) {
                return v;
            } else {
                Util.checkErrorCode(rc);
                return null;
            }
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    public boolean remove(Transaction tx, K k, V v) {
        final int kSz = bitsToBytes(kSchema.sizeBits(k));
        final int vSz = bitsToBytes(vSchema.sizeBits(v));

        final long kBufferPtrNow = allocateBufferPointer(kBufferPtr, kSz);
        fillBufferPointerFromSchema(kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = allocateBufferPointer(vBufferPtr, vSz);
        fillBufferPointerFromSchema(vSchema, vBufferPtrNow, vSz, v);
        try {
            int rc = JNI.mdb_del(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        } finally {
            freeBufferPointer(vBufferPtr, vBufferPtrNow);
            freeBufferPointer(kBufferPtr, kBufferPtrNow);
            tx.generation++;
        }
    }

    public boolean contains(Transaction tx, K k, V v) {
        // Unfortunately when using MDB_DUPSORT mdb_get ignores the data parameter and just
        // returns the first value associated with a key.
        try (CursorWithDuplicateKeys<K, V> cursor = createCursor(tx)) {
            return cursor.moveTo(k, v);
        }
    }

    // Override the base class so that we don't get duplicate keys in the iterator
    @Override
    public Iterator<K> keys(Transaction tx) {
        final CursorWithDuplicateKeys<K, V> cursor = createCursor(tx);
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
                hasNext = cursor.moveFirstOfNextKey();
                if (!hasNext) {
                    cursor.close();
                }
                return key;
            }
        };
    }
}
