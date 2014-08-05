package uk.co.omegaprime.thunder;

public class DatabaseOptions {
    int createPermissions = 0644;
    long mapSizeBytes = 10_485_760;
    long maxIndexes   = 1;
    long maxReaders   = 126;
    int flags         = JNI.MDB_WRITEMAP;

    public DatabaseOptions createPermissions(int perms) { this.createPermissions = perms; return this; }
    public DatabaseOptions mapSize(long bytes)          { this.mapSizeBytes = bytes; return this; }
    public DatabaseOptions maxIndexes(long indexes)     { this.maxIndexes = indexes; return this; }
    public DatabaseOptions maxReaders(long readers)     { this.maxReaders = readers; return this; }

    private DatabaseOptions flag(int flag, boolean set) { this.flags = set ? flags | flag : flags & ~flag; return this; }
    public DatabaseOptions writeMap(boolean set)       { return flag(JNI.MDB_WRITEMAP,   set); }
    public DatabaseOptions noSubDirectory(boolean set) { return flag(JNI.MDB_NOSUBDIR,   set); }
    public DatabaseOptions readOnly(boolean set)       { return flag(JNI.MDB_RDONLY,     set); }
    public DatabaseOptions noTLS(boolean set)          { return flag(JNI.MDB_NOTLS,      set); }
}
