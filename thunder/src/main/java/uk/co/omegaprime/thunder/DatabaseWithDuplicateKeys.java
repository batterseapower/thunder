package uk.co.omegaprime.thunder;

import uk.co.omegaprime.thunder.schema.Schema;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;

public class DatabaseWithDuplicateKeys<K, V> extends Database<K, V> {
    private final UntypedDatabaseWithDuplicateKeys udb;

    public DatabaseWithDuplicateKeys(UntypedDatabaseWithDuplicateKeys udb, Schema<K> kSchema, Schema<V> vSchema) {
        super(udb, kSchema, vSchema);
        this.udb = udb;
    }

    DatabaseWithDuplicateKeys(UntypedDatabaseWithDuplicateKeys udb, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer) {
        super(udb, kBuffer, vBuffer);
        this.udb = udb;
    }

    @Override
    public UntypedDatabaseWithDuplicateKeys getUntypedDatabase() { return udb; }

    public CursorWithDuplicateKeys<K, V> createCursor(Transaction tx) {
        return new CursorWithDuplicateKeys<>(this, udb.createCursor(tx));
    }

    public boolean remove(Transaction tx, K k, V v) {
        return udb.remove(tx, kBuffer, vBuffer, k, v);
    }

    public boolean contains(Transaction tx, K k, V v) {
        return udb.contains(tx, kBuffer, vBuffer,  k, v);
    }
}
