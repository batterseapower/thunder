package uk.co.omegaprime.thunder;

import com.sun.webkit.SharedBuffer;
import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

// XXX: type specialisation for true 0-allocation? But we might hope that escape analysis would save us because our boxes are intermediate only.
public class Cursor<K, V> implements Cursorlike<K,V> {
    private final Database<K, V> database;
    private final UntypedCursor utc;

    public Cursor(Database<K, V> database, UntypedCursor utc) {
        this.database = database;
        this.utc = utc;
    }

    boolean keyStartsWith(K k) {
        return utc.keyStartsWith(database.kBuffer, k);
    }

    @Override public boolean moveFirst()    { return utc.moveFirst(); }
    @Override public boolean moveLast()     { return utc.moveLast(); }
    @Override public boolean moveNext()     { return utc.moveNext(); }
    @Override public boolean movePrevious() { return utc.movePrevious(); }
    @Override public boolean isPositioned() { return utc.isPositioned(); }

    @Override public boolean moveTo(K k)      { return utc.moveTo(database.kBuffer, k); }
    @Override public boolean moveCeiling(K k) { return utc.moveCeiling(database.kBuffer, k); }
    @Override public boolean moveFloor(K k)   { return utc.moveFloor(database.kBuffer, k); }

    @Override
    public K getKey() { return utc.getKey(database.kBuffer); }

    @Override
    public V getValue() { return utc.getValue(database.vBuffer); }

    @Override
    public void put(V v) { utc.put(database.kBuffer, database.vBuffer, v); }

    @Override
    public void put(K k, V v) { utc.put(database.kBuffer, database.vBuffer, k, v); }

    @Override
    public V putIfAbsent(K k, V v) { return utc.putIfAbsent(database.kBuffer, database.vBuffer, k, v); }

    @Override
    public void delete() { utc.delete(); }

    public <K2, V2> Cursor<K2, V2> reinterpretView(Schema<K2> k2Schema, Schema<V2> v2Schema) {
        return new Cursor<>(new Database<>(database.getUntypedDatabase(), k2Schema, v2Schema), utc);
    }

    @Override public Schema<K> getKeySchema()   { return database.getKeySchema(); }
    @Override public Schema<V> getValueSchema() { return database.getValueSchema(); }

    @Override
    public Database<K, V> getDatabase() { return database; }
}
