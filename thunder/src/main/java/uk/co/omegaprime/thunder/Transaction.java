package uk.co.omegaprime.thunder;

public class Transaction implements AutoCloseable {
    final long txn;
    boolean handleFreed = false;

    public Transaction(long txn) {
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
