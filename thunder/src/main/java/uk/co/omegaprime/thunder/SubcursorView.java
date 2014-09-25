package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;

public class SubcursorView<K1, K2, V> implements Cursorlike<K2, V> {
    private final Schema<K1> k1Schema;
    private final Schema<K2> k2Schema;

    private final Cursor<?, V> cursor;
    private final Cursor<Pair<K1, K2>, V> k1k2SeekView;
    private final Cursor<K1, V> k1SeekView;
    private final Cursor<K1, V> k1SuccSeekView;

    private K1 k1;
    private boolean k1IsMaximum;

    // FIXME: only allow this to be used if the current key schema is a pair schema?
    // FIXME: version for cursors with duplicate keys?
    // TODO: would be cooler if the schema could be inferred from the key type of the cursor..
    public SubcursorView(Cursor<?, V> cursor, Schema<K1> k1Schema, Schema<K2> k2Schema) {
        this.cursor = cursor;
        this.k1Schema = k1Schema;
        this.k2Schema = k2Schema;

        final Schema<K1> k1SuccSchema = new Schema<K1>() {
            @Override
            public K1 read(BitStream bs) {
                throw new IllegalStateException("k1SuccSchema.read");
            }

            @Override
            public int maximumSizeBits() {
                return k1Schema.maximumSizeBits();
            }

            @Override
            public int sizeBits(K1 k1) {
                return k1Schema.sizeBits(k1);
            }

            @Override
            public void write(BitStream bs, K1 k1) {
                long mark = bs.mark();
                k1Schema.write(bs, k1);
                bs.incrementBitStreamFromMark(mark);
            }
        };

        this.k1k2SeekView   = cursor.reinterpretView(Schema.zip(k1Schema, k2Schema), cursor.getValueSchema());
        // NB: it is for this purpose that seekView.keyEquals carefully does *not* compare the trailing bits of the
        // key/value as well -- for k1SeekView those trailing bits would be the first bits of "b" so may be non-zero
        this.k1SeekView     = cursor.reinterpretView(k1Schema,     cursor.getValueSchema());
        this.k1SuccSeekView = cursor.reinterpretView(k1SuccSchema, cursor.getValueSchema());
    }

    public SubcursorView(Cursor<?, V> cursor, Schema<K1> k1Schema, Schema<K2> k2Schema, K1 k1) {
        this(cursor, k1Schema, k2Schema);
        setPosition(k1);
    }

    public void setPosition(K1 k1) {
        // Bit of a hack here unfortunately...
        final Index<?, V> index = cursor.index;
        final int k1Sz = bitsToBytes(k1Schema.sizeBits(k1));
        final long aBufferPtrNow = Index.allocateBufferPointer(index.kBufferPtr, k1Sz);
        try {
            index.bs.initialize(aBufferPtrNow + 2 * Unsafe.ADDRESS_SIZE, k1Sz);
            final long mark = index.bs.mark();
            k1Schema.write(index.bs, k1);
            k1IsMaximum = index.bs.incrementBitStreamFromMark(mark);
        } finally {
            Index.freeBufferPointer(index.kBufferPtr, aBufferPtrNow);
        }

        this.k1 = k1;
    }

    public K1 getPosition() {
        return k1;
    }

    @Override
    public boolean moveFirst() {
        return k1SeekView.moveCeiling(k1) && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public boolean moveLast() {
        return (!k1IsMaximum && k1SuccSeekView.moveCeiling(k1) ? k1SeekView.movePrevious() : k1SeekView.moveLast()) && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public boolean moveNext() {
        return k1SeekView.moveNext() && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public boolean movePrevious() {
        return k1SeekView.movePrevious() && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public boolean moveTo(K2 k2) {
        return k1k2SeekView.moveTo(new Pair<>(k1, k2));
    }

    @Override
    public boolean moveCeiling(K2 k2) {
        return k1k2SeekView.moveCeiling(new Pair<>(k1, k2)) && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public boolean moveFloor(K2 k2) {
        return k1k2SeekView.moveFloor(new Pair<>(k1, k2)) && k1SeekView.keyStartsWith(k1);
    }

    @Override
    public K2 getKey() {
        return k1k2SeekView.getKey().v;
    }

    @Override
    public V getValue() {
        return k1k2SeekView.getValue();
    }

    @Override
    public void put(V v) {
        k1k2SeekView.put(v);
    }

    @Override
    public void put(K2 k2, V v) {
        k1k2SeekView.put(new Pair<>(k1, k2), v);
    }

    @Override
    public V putIfAbsent(K2 k2, V v) {
        return k1k2SeekView.putIfAbsent(new Pair<>(k1, k2), v);
    }

    @Override
    public void delete() {
        k1k2SeekView.delete();
    }

    @Override public Schema<K2> getKeySchema() { return k2Schema; }
    @Override public Schema<V> getValueSchema() { return cursor.getValueSchema(); }

    /*
    public <C, D> SubcursorView<Pair<K1, C>, D, V> subcursorView(Schema<C> cSchema, Schema<D> dSchema, final C c) {
        // We are SubcursorView<K1, K2, V>
        //
        // B == C + D
        // K == A + B == A + C + D
        final SubcursorView<Pair<K1, C>, D, V> result = new SubcursorView<Pair<K1, C>, D, V>(cursor, Schema.<K1, C>zip(k1Schema, cSchema), dSchema);
        result.setPosition(new Pair<K1, C>(k1, c));
        return result;
    }
    */
}
