package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class UntypedDatabase {
    final Environment db;
    final long dbi;

    public UntypedDatabase(Environment db, long dbi) {
        this.db = db;
        this.dbi = dbi; // NB: we never mdb_dbi_close. This should be safe, and avoids Database having to be AutoCloseable
    }

    public UntypedCursor createCursor(Transaction tx) {
        final long[] cursorPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_open(tx.txn, dbi, cursorPtr));
        return new UntypedCursor(this, tx, cursorPtr[0]);
    }

    public <K, V> void put(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        unsafe.putAddress(vBufferPtrNow, vSz);
        try {
            Util.checkErrorCode(JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE));
            assert(unsafe.getAddress(vBufferPtrNow) == vSz);
            vBuffer.writeDirect(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public <K, V> V putIfAbsent(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k, V v) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));
        final int vSz = bitsToBytes(vBuffer.getSchema().sizeBits(v));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(vSz);
        vBuffer.write(vBufferPtrNow, vSz, v);
        try {
            int rc = JNI.mdb_put(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow, JNI.MDB_RESERVE | JNI.MDB_NOOVERWRITE);
            if (rc == JNI.MDB_KEYEXIST) {
                return vBuffer.read(vBufferPtrNow);
            } else {
                Util.checkErrorCode(rc);
                assert(unsafe.getAddress(vBufferPtrNow) == vSz);
                vBuffer.writeDirect(unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE), vSz, v);
                return null;
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public <K> boolean remove(Transaction tx, BufferedSchema<K> kBuffer, K k) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
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

    public <K, V> V get(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = vBuffer.allocate(0);
        try {
            int rc = JNI.mdb_get(tx.txn, dbi, kBufferPtrNow, vBufferPtrNow);
            if (rc == JNI.MDB_NOTFOUND) {
                return null;
            } else {
                Util.checkErrorCode(rc);
                return vBuffer.read(vBufferPtrNow);
            }
        } finally {
            vBuffer.free(vBufferPtrNow);
            kBuffer.free(kBufferPtrNow);
        }
    }

    public <K, V> boolean contains(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer, K k) {
        final int kSz = bitsToBytes(kBuffer.getSchema().sizeBits(k));

        final long kBufferPtrNow = kBuffer.allocate(kSz);
        kBuffer.write(kBufferPtrNow, kSz, k);
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

    public <K> Iterator<K> keys(Transaction tx, BufferedSchema<K> kBuffer) {
        final UntypedCursor cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<K>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public K next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final K key = cursor.getKey(kBuffer);
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return key;
            }
        };
    }

    public <V> Iterator<V> values(Transaction tx, BufferedSchema<V> vBuffer) {
        final UntypedCursor cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<V>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public V next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final V value = cursor.getValue(vBuffer);
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return value;
            }
        };
    }

    public <K, V> Iterator<Pair<K, V>> keyValues(Transaction tx, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer) {
        final UntypedCursor cursor = createCursor(tx);
        final boolean initialHasNext = cursor.moveFirst();
        return new Iterator<Pair<K, V>>() {
            boolean hasNext = initialHasNext;

            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public Pair<K, V> next() {
                if (!hasNext) throw new IllegalStateException("No more elements");

                final Pair<K, V> pair = new Pair<>(cursor.getKey(kBuffer), cursor.getValue(vBuffer));
                hasNext = cursor.moveNext();
                if (!hasNext) {
                    cursor.close();
                }
                return pair;
            }
        };
    }
}
