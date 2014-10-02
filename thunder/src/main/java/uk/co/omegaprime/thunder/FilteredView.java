package uk.co.omegaprime.thunder;

import uk.co.omegaprime.thunder.schema.Schema;

import java.util.function.BiFunction;

public class FilteredView<K, V> implements Cursorlike<K, V> {
    private final Cursorlike<K, V> cursor;
    private final BiFunction<? super K, ? super V, Boolean> predicate;

    public FilteredView(Cursorlike<K, V> cursor, BiFunction<? super K, ? super V, Boolean> predicate) {
        this.cursor = cursor;
        this.predicate = predicate;
    }

    private boolean seekForwardForMatch() {
        do {
            if (matches()) return true;
        } while (cursor.moveNext());

        return false;
    }

    private boolean seekBackwardForMatch() {
        do {
            if (matches()) return true;
        } while (cursor.movePrevious());

        return false;
    }

    private boolean matches() {
        return predicate.apply(getKey(), getValue());
    }

    @Override
    public boolean moveFirst() { return cursor.moveFirst() && seekForwardForMatch(); }

    @Override
    public boolean moveLast() { return cursor.moveLast() && seekBackwardForMatch(); }

    @Override
    public boolean moveNext() { return cursor.moveNext() && seekForwardForMatch(); }

    @Override
    public boolean movePrevious() { return cursor.movePrevious() && seekBackwardForMatch(); }

    @Override
    public boolean moveTo(K k) { return cursor.moveTo(k) && matches(); }

    @Override
    public boolean moveCeiling(K k) { return cursor.moveCeiling(k) && seekForwardForMatch(); }

    @Override
    public boolean moveFloor(K k) { return cursor.moveFloor(k) && seekBackwardForMatch(); }

    @Override
    public boolean isPositioned() { return cursor.isPositioned(); }

    @Override
    public K getKey() { return cursor.getKey(); }

    @Override
    public V getValue() { return cursor.getValue(); }

    @Override
    public void put(V v) { cursor.put(v); }

    @Override
    public void put(K k, V v) { cursor.put(k, v); }

    @Override
    public V putIfAbsent(K k, V v) { return cursor.putIfAbsent(k, v); }

    @Override
    public void delete() { cursor.delete(); }

    @Override public Schema<K> getKeySchema() { return cursor.getKeySchema(); }
    @Override public Schema<V> getValueSchema() { return cursor.getValueSchema(); }

    @Override
    public Database<?, ?> getDatabase() { return cursor.getDatabase(); }
}
