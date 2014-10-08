package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;
import uk.co.omegaprime.thunder.schema.VoidSchema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

// Holds a pointer to a scratch mdb_val structure to avoid us having to reallocate one upon every call.
// Unlike Schema (which is immutable and thread safe) this class is mutable and not thread safe.
final class BufferedSchema<T> {
    public static final BufferedSchema<Void> VOID = new BufferedSchema<Void>(VoidSchema.INSTANCE, 0);

    private final Schema<T> schema;
    private final long bufferPtr;
    final BitStream bs = new BitStream();

    public BufferedSchema(Schema<T> schema) {
        // TODO: speculatively allocate a reasonable amount of memory that most allocations of interest might fit into?
        this(schema, schema.maximumSizeBits() < 0 ? 0 : allocateMdbVal(bitsToBytes(schema.maximumSizeBits())));
    }

    private BufferedSchema(Schema<T> schema, long bufferPtr) {
        this.schema = schema;
        this.bufferPtr = bufferPtr;
    }

    public Schema<T> getSchema() {
        return schema;
    }

    @Override
    public void finalize() throws Throwable {
        if (bufferPtr != 0) {
            unsafe.freeMemory(bufferPtr);
        }
        super.finalize();
    }

    private static long allocateMdbVal(int sz) {
        return unsafe.allocateMemory(2 * Unsafe.ADDRESS_SIZE + sz);
    }

    public long allocate(int sz) {
        if (bufferPtr != 0) {
            return bufferPtr;
        } else {
            return allocateMdbVal(sz);
        }
    }

    public long allocateAndCopy(long bufferPtrToCopy) {
        int sz = (int)unsafe.getAddress(bufferPtrToCopy);
        long bufferPtrNow = allocate(sz);
        unsafe.putAddress(bufferPtrNow,                       sz);
        unsafe.putAddress(bufferPtrNow + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
        unsafe.copyMemory(unsafe.getAddress(bufferPtrToCopy + Unsafe.ADDRESS_SIZE), bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz);
        return bufferPtrNow;
    }

    public void free(long bufferPtrNow) {
        if (bufferPtr == 0) {
            unsafe.freeMemory(bufferPtrNow);
        }
    }

    // INVARIANT: sz == schema.size(x)
    public void write(long bufferPtr, int sz, T x) {
        unsafe.putAddress(bufferPtr, sz);
        unsafe.putAddress(bufferPtr + Unsafe.ADDRESS_SIZE, bufferPtr + 2 * Unsafe.ADDRESS_SIZE);
        writeDirect(bufferPtr + 2 * Unsafe.ADDRESS_SIZE, sz, x);
    }

    public void writeDirect(long bufferPtr, int sz, T x) {
        bs.initialize(bufferPtr, sz);
        schema.write(bs, x);
        bs.zeroFill();
    }

    public T read(long bufferPtr) {
        bs.initialize(unsafe.getAddress(bufferPtr + Unsafe.ADDRESS_SIZE), (int)unsafe.getAddress(bufferPtr));
        return schema.read(bs);
    }
}
