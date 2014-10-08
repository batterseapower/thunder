package uk.co.omegaprime.thunder;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;

public class UntypedDatabaseWithDuplicateKeys extends UntypedDatabase {
    public UntypedDatabaseWithDuplicateKeys(Environment env, long dbi) {
        super(env, dbi);
    }

    public UntypedCursorWithDuplicateKeys createCursor(Transaction tx) {
        final long[] cursorPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
        return new UntypedCursorWithDuplicateKeys(this, tx, cursorPtr[0]);
    }

    // Override the base class because MDB_RESERVE doesn't really make sense with MDB_DUPSORT
    @Override
    public <K, V>void put(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        vBuffer.write(vBufferPtrNow, vSz, v);
        try {
            Util.checkErrorCode(JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, 0));
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    // Override because MDB_RESERVE doesn't work, and need to use MDB_NODUPDATA rather than MDB_NOOVERWRITE
    @Override
    public <K, V> V putIfAbsent(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        vBuffer.write(vBufferPtrNow, vSz, v);
        try {
            final int rc = JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_NODUPDATA);
            if (rc == JNI.MDB_KEYEXIST) {
                return v;
            } else {
                Util.checkErrorCode(rc);
                return null;
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public <K, V> boolean remove(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        vBuffer.write(vBufferPtrNow, vSz, v);
        try {
            int rc = JNI.mdb_del(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return false;
            } else {
                Util.checkErrorCode(rc);
                return true;
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public <K, V> boolean contains(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        // Unfortunately when using MDB_DUPSORT mdb_get ignores the data parameter and just
        // returns the first value associated with a key.
        try (UntypedCursorWithDuplicateKeys uc = createCursor(tx)) {
            return new CursorWithDuplicateKeys<>(new DatabaseWithDuplicateKeys<>(this, kBuffer, vBuffer), createCursor(tx)).moveTo(k, v);
        }
    }

    // Override the base class so that we don't get duplicate keys in the iterator
    @Override
    public <K> Iterator<K> keys(Transaction tx, BufferedSchema<K> kBuffer) {
        final UntypedCursorWithDuplicateKeys uc = createCursor(tx);
        final CursorWithDuplicateKeys<K, Void> cursor = new CursorWithDuplicateKeys<>(new DatabaseWithDuplicateKeys<>(this, kBuffer, BufferedSchema.VOID), uc);
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
                    uc.close();
                }
                return key;
            }
        };
    }
}
