package uk.co.omegaprime.thunder;

import uk.co.omegaprime.thunder.schema.Schema;

public interface Cursorlike<K, V> {
    boolean moveFirst();
    boolean moveLast();
    boolean moveNext();
    boolean movePrevious();

    boolean moveTo(K k);
    boolean moveCeiling(K k);
    boolean moveFloor(K k);

    boolean isPositioned();

    K getKey();

    V getValue();

    void put(V v);

    void put(K k, V v);

    V putIfAbsent(K k, V v);

    void delete();

    Schema<K> getKeySchema();
    Schema<V> getValueSchema();

    Database<?, ?> getDatabase();
}
