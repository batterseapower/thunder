package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import java.util.Iterator;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

public class Database<K, V> {
    private final UntypedDatabase udb;

    // Used for temporary scratch storage within the context of a single method only, basically
    // just to save some calls to the allocator. The sole reason why Database is not thread safe.
    final BufferedSchema<K> kBuffer;
    final BufferedSchema<V> vBuffer;

    public Database(UntypedDatabase udb, Schema<K> kSchema, Schema<V> vSchema) {
        this(udb, new BufferedSchema<>(kSchema), new BufferedSchema<>(vSchema));
    }

    Database(UntypedDatabase udb, BufferedSchema<K> kBuffer, BufferedSchema<V> vBuffer) {
        this.udb = udb;
        this.kBuffer = kBuffer;
        this.vBuffer = vBuffer;
    }

    public UntypedDatabase getUntypedDatabase() { return udb; }

    public Schema<K> getKeySchema()   { return kBuffer.getSchema(); }
    public Schema<V> getValueSchema() { return vBuffer.getSchema(); }

    public Cursor<K, V> createCursor(Transaction tx) {
        return new Cursor<>(this, udb.createCursor(tx));
    }

    public void put(Transaction tx, K k, V v) {
        udb.put(tx, kBuffer, vBuffer, k, v);
    }

    public V putIfAbsent(Transaction tx, K k, V v) {
        return udb.putIfAbsent(tx, kBuffer, vBuffer, k, v);
    }

    public boolean remove(Transaction tx, K k) {
        return udb.remove(tx, kBuffer, k);
    }

    public V get(Transaction tx, K k) {
        return udb.get(tx, kBuffer, vBuffer, k);
    }

    public boolean contains(Transaction tx, K k) {
        return udb.contains(tx, kBuffer, vBuffer, k);
    }

    public Iterator<K> keys(Transaction tx) {
        return udb.keys(tx, kBuffer);
    }

    public Iterator<V> values(Transaction tx) {
        return udb.values(tx, vBuffer);
    }

    public Iterator<Pair<K, V>> keyValues(Transaction tx) {
        return udb.keyValues(tx, kBuffer, vBuffer);
    }
}
