package uk.co.omegaprime.thunder;

// LMDB transactions may not span threads (except in the special case of MDB_NOTLS and read-only transactions,
// which we don't have any special handling for), so this class is not thread safe
public final class Transaction implements AutoCloseable {
    final long txn;
    boolean handleFreed = false;

    // The generation number is incremented by 1 every time we update the database. This lets us decide
    // when the bufferPtr cached by a Cursor has potentially gone stale and must be fetched anew.
    long generation = 0;

    Transaction(long txn) {
        this.txn = txn;
    }

    public void abort() {
        handleFreed = true;
        JNI.mdb_txn_abort(txn);
    }

    public void commit() {
        handleFreed = true;
        Util.checkErrorCode(JNI.mdb_txn_commit(txn));
    }

    public void close() {
        if (!handleFreed) {
            // Get a very scary JVM crash if we call this after already calling commit()
            abort();
        }
    }
}
