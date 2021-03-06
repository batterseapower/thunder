package uk.co.omegaprime.thunder;

import uk.co.omegaprime.thunder.schema.Schema;

import java.io.File;

public class Environment implements AutoCloseable {
    final long env;

    public Environment(File file) {
        this(file, new EnvironmentOptions());
    }

    public Environment(File file, EnvironmentOptions options) {
        final long[] envPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_env_create(envPtr));
        env = envPtr[0];

        Util.checkErrorCode(JNI.mdb_env_set_maxdbs(env, options.maxDatabases));
        Util.checkErrorCode(JNI.mdb_env_set_mapsize(env, options.mapSizeBytes));
        Util.checkErrorCode(JNI.mdb_env_set_maxreaders(env, options.maxReaders));

        Util.checkErrorCode(JNI.mdb_env_open(env, file.getAbsolutePath(), options.flags, options.createPermissions));
    }

    public void setMetaSync(boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_NOMETASYNC, enabled ? 0 : 1)); }
    public void setSync    (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_NOSYNC,     enabled ? 0 : 1)); }
    public void setMapSync (boolean enabled) { Util.checkErrorCode(JNI.mdb_env_set_flags(env, JNI.MDB_MAPASYNC,   enabled ? 0 : 1)); }

    public void sync(boolean force) { Util.checkErrorCode(JNI.mdb_env_sync(env, force ? 1 : 0)); }

    public <K, V> Database<K, V> database(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
        return database(tx, name, kSchema, vSchema, false);
    }
    public <K, V> Database<K, V> createDatabase(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
        return database(tx, name, kSchema, vSchema, true);
    }
    public <K, V> Database<K, V> database(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema, boolean allowCreation) {
        final long[] dbiPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_dbi_open(tx.txn, name, allowCreation ? JNI.MDB_CREATE : 0, dbiPtr));
        return new Database<>(new UntypedDatabase(this, dbiPtr[0]), kSchema, vSchema);
    }

    public <K, V> DatabaseWithDuplicateKeys<K, V> databaseWithDuplicateKeys(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
        return databaseWithDuplicateKeys(tx, name, kSchema, vSchema, false);
    }
    public <K, V> DatabaseWithDuplicateKeys<K, V> createDatabaseWithDuplicateKeys(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema) {
        return databaseWithDuplicateKeys(tx, name, kSchema, vSchema, true);
    }
    public <K, V> DatabaseWithDuplicateKeys<K, V> databaseWithDuplicateKeys(Transaction tx, String name, Schema<K> kSchema, Schema<V> vSchema, boolean allowCreation) {
        final long[] dbiPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_dbi_open(tx.txn, name, JNI.MDB_DUPSORT | (allowCreation ? JNI.MDB_CREATE : 0), dbiPtr));
        return new DatabaseWithDuplicateKeys<>(new UntypedDatabaseWithDuplicateKeys(this, dbiPtr[0]), kSchema, vSchema);
    }

    // Quoth the docs:
    //   A transaction and its cursors must only be used by a single
    //   thread, and a thread may only have a single transaction at a time.
    //   If #MDB_NOTLS is in use, this does not apply to read-only transactions.
    public Transaction transaction(boolean isReadOnly) {
        final long[] txnPtr = new long[1];
        Util.checkErrorCode(JNI.mdb_txn_begin(env, 0, isReadOnly ? JNI.MDB_RDONLY : 0, txnPtr));
        return new Transaction(txnPtr[0]);
    }

    public void close() {
        JNI.mdb_env_close(env);
    }
}
