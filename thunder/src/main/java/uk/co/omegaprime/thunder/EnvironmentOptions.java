package uk.co.omegaprime.thunder;

public class EnvironmentOptions {
    int createPermissions = 0644;
    long mapSizeBytes = 10_485_760;
    long maxDatabases = 1;
    long maxReaders   = 126;
    int flags         = JNI.MDB_WRITEMAP;

    public EnvironmentOptions createPermissions(int perms) { this.createPermissions = perms; return this; }
    public EnvironmentOptions mapSize(long bytes)          { this.mapSizeBytes = bytes; return this; }
    public EnvironmentOptions maxDatabases(long databases) { this.maxDatabases = databases; return this; }
    public EnvironmentOptions maxReaders(long readers)     { this.maxReaders = readers; return this; }

    private EnvironmentOptions flag(int flag, boolean set) { this.flags = set ? flags | flag : flags & ~flag; return this; }
    public EnvironmentOptions writeMap(boolean set)       { return flag(JNI.MDB_WRITEMAP,   set); }
    public EnvironmentOptions noSubDirectory(boolean set) { return flag(JNI.MDB_NOSUBDIR,   set); }
    public EnvironmentOptions readOnly(boolean set)       { return flag(JNI.MDB_RDONLY,     set); }
    public EnvironmentOptions noTLS(boolean set)          { return flag(JNI.MDB_NOTLS,      set); }
}
