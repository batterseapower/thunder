package uk.co.omegaprime.thunder;

public interface Cursorlike<K, V> {
    boolean moveFirst();

    boolean moveLast();

    boolean moveNext();

    boolean movePrevious();

    boolean moveTo(K k);

    boolean moveCeiling(K k);

    boolean moveFloor(K k);

    K getKey();

    V getValue();

    void put(V v);

    void put(K k, V v);

    V putIfAbsent(K k, V v);

    void delete();
}
