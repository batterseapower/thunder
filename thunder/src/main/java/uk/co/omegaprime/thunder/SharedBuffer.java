package uk.co.omegaprime.thunder;

import sun.misc.Unsafe;
import uk.co.omegaprime.thunder.schema.Schema;

import static uk.co.omegaprime.thunder.Bits.bitsToBytes;
import static uk.co.omegaprime.thunder.Bits.unsafe;

// Holds a pointer to a scratch mdb_val structure to avoid us having to reallocate one upon every call
final class SharedBuffer implements AutoCloseable {
    private final long bufferPtr;

    public SharedBuffer(Schema<?> schema) {
        if (schema.maximumSizeBits() < 0) {
            // TODO: speculatively allocate a reasonable amount of memory that most allocations of interest might fit into?
            bufferPtr = 0;
        } else {
            bufferPtr = allocateMdbVal(bitsToBytes(schema.maximumSizeBits()));
        }
    }

    public void close() {
        if (bufferPtr != 0) {
            unsafe.freeMemory(bufferPtr);
        }
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
}
