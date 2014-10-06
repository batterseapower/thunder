package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class CursorWithDuplicateKeys<K, V> extends Cursor<K, V> implements AutoCloseable {
    public CursorWithDuplicateKeys(DatabaseWithDuplicateKeys<K, V> database, Transaction tx, long cursor) {
        super(database, tx, cursor);
    }

    public boolean moveFirstOfKey()        { return move(JNI.MDB_FIRST_DUP); }
    public boolean moveLastOfKey()         { return move(JNI.MDB_LAST_DUP); }
    public boolean moveNextOfKey()         { return move(JNI.MDB_NEXT_DUP); }
    public boolean movePreviousOfKey()     { return move(JNI.MDB_PREV_DUP); }
    public boolean moveFirstOfNextKey()    { return move(JNI.MDB_NEXT_NODUP); }
    public boolean moveLastOfPreviousKey() { return move(JNI.MDB_PREV_NODUP); }

    public boolean moveTo(K k, V v)           { return move(k, v, JNI.MDB_GET_BOTH); }
    public boolean moveCeilingOfKey(K k, V v) { return move(k, v, JNI.MDB_GET_BOTH_RANGE); }

    public boolean moveFloorOfKey(K k, V v) {
        if (moveCeilingOfKey(k, v)) {
            return valueEquals(v) || movePreviousOfKey();
        } else {
            return moveTo(k) && moveLastOfKey();
        }
    }

    public long keyItemCount() {
        final long[] output = new long[1];
        Util.checkErrorCode(JNI.mdb_cursor_count(cursor, output));
        return output[0];
    }

    private boolean move(K k, V v, int op) {
        final int kSz = bitsToBytes(database.kSchema.sizeBits(k));
        final int vSz = bitsToBytes(database.vSchema.sizeBits(v));

        final long kBufferPtrNow = database.kBuffer.allocate(kSz);
        database.fillBufferPointerFromSchema(database.kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = database.vBuffer.allocate(vSz);
        database.fillBufferPointerFromSchema(database.vSchema, vBufferPtrNow, vSz, v);
        try {
            return isFound(JNI.mdb_cursor_get(cursor, kBufferPtrNow, vBufferPtrNow, op));
        } finally {
            // Need to copy the MDB_vals from the temp structure to the permanent one, in case someone does getKey() now (they should get back k)
            unsafe.putAddress(shared.bufferPtr,                           unsafe.getAddress(kBufferPtrNow));
            unsafe.putAddress(shared.bufferPtr +     Unsafe.ADDRESS_SIZE, unsafe.getAddress(kBufferPtrNow + Unsafe.ADDRESS_SIZE));
            unsafe.putAddress(shared.bufferPtr + 2 * Unsafe.ADDRESS_SIZE, unsafe.getAddress(vBufferPtrNow));
            unsafe.putAddress(shared.bufferPtr + 3 * Unsafe.ADDRESS_SIZE, unsafe.getAddress(vBufferPtrNow + Unsafe.ADDRESS_SIZE));
            database.kBuffer.free(kBufferPtrNow);
            shared.bufferPtrGeneration = tx.generation;
        }
    }

    // Override the base class because MDB_RESERVE doesn't really make sense with duplicates
    @Override
    public void put(K k, V v) {
        final int kSz = bitsToBytes(database.kSchema.sizeBits(k));
        final int vSz = bitsToBytes(database.vSchema.sizeBits(v));

        final long kBufferPtrNow = database.kBuffer.allocate(kSz);
        database.fillBufferPointerFromSchema(database.kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = database.vBuffer.allocate(vSz);
        database.fillBufferPointerFromSchema(database.vSchema, vBufferPtrNow, vSz, v);
        try {
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, 0));
        } finally {
            database.vBuffer.free(vBufferPtrNow);
            database.kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    // Override the base class because MDB_RESERVE doesn't really make sense with duplicates
    @Override
    public V putIfAbsent(K k, V v) {
        final int kSz = bitsToBytes(database.kSchema.sizeBits(k));
        final int vSz = bitsToBytes(database.vSchema.sizeBits(v));

        final long kBufferPtrNow = database.kBuffer.allocate(kSz);
        database.fillBufferPointerFromSchema(database.kSchema, kBufferPtrNow, kSz, k);
        final long vBufferPtrNow = database.vBuffer.allocate(vSz);
        database.fillBufferPointerFromSchema(database.vSchema, vBufferPtrNow, vSz, v);
        try {
            final int rc = JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, JNI.MDB_NODUPDATA);
            if (rc == JNI.MDB_KEYEXIST) {
                return v;
            } else {
                Util.checkErrorCode(rc);
                return null;
            }
        } finally {
            database.vBuffer.free(vBufferPtrNow);
            database.kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    // Override the base class because MDB_RESERVE doesn't really make sense with duplicates.
    // Furthermore, MDB_CURRENT isn't actually useful for anything when using MDB_DUPSORT!
    @Override
    public void put(V v) {
        refreshBufferPtr();

        final int vSz = bitsToBytes(database.vSchema.sizeBits(v));

        // See the comment in Cursor.put that explains why we have to "needlessly" copy the key from
        // bufferPtr into a fresh buffer for the call to mdb_cursor_put.
        final long kBufferPtrNow = database.kBuffer.allocateAndCopy(shared.bufferPtr);
        final long vBufferPtrNow = database.vBuffer.allocate(vSz);
        database.fillBufferPointerFromSchema(database.vSchema, vBufferPtrNow, vSz, v);
        try {
            Util.checkErrorCode(JNI.mdb_cursor_put(cursor, kBufferPtrNow, vBufferPtrNow, 0));
        } finally {
            database.vBuffer.free(vBufferPtrNow);
            database.kBuffer.free(kBufferPtrNow);
            tx.generation++;
        }
    }

    public void deleteAllOfKey() {
        Util.checkErrorCode(JNI.mdb_cursor_del(cursor, JNI.MDB_NODUPDATA));
        tx.generation++;
    }
}
