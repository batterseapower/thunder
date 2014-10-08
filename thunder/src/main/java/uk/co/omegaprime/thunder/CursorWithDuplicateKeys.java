package uk.co.omegaprime.thunder;

public class CursorWithDuplicateKeys<K, V> extends Cursor<K, V> {
    private final DatabaseWithDuplicateKeys<K, V> database;
    private final UntypedCursorWithDuplicateKeys utc;

    public CursorWithDuplicateKeys(DatabaseWithDuplicateKeys<K, V> database, UntypedCursorWithDuplicateKeys utc) {
        super(database, utc);
        this.database = database;
        this.utc = utc;
    }

    public boolean moveFirstOfKey()        { return utc.moveFirstOfKey(); }
    public boolean moveLastOfKey()         { return utc.moveLastOfKey(); }
    public boolean moveNextOfKey()         { return utc.moveNextOfKey(); }
    public boolean movePreviousOfKey()     { return utc.movePreviousOfKey(); }
    public boolean moveFirstOfNextKey()    { return utc.moveFirstOfNextKey(); }
    public boolean moveLastOfPreviousKey() { return utc.moveLastOfPreviousKey(); }

    public boolean moveTo(K k, V v)           { return utc.moveTo(database.kBuffer, database.vBuffer, k, v); }
    public boolean moveCeilingOfKey(K k, V v) { return utc.moveCeilingOfKey(database.kBuffer, database.vBuffer, k, v); }
    public boolean moveFloorOfKey(K k, V v)   { return utc.moveFloorOfKey(database.kBuffer, database.vBuffer, k, v); }

    public long keyItemCount() { return utc.keyItemCount(); }

    public void deleteAllOfKey() { utc.deleteAllOfKey(); }
}
