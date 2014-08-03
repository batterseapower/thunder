package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;

import java.util.ArrayList;
import java.util.List;

public class ListSchema {
    public static <T> Schema<List<T>> of(Schema<T> schema) {
        return new Schema<List<T>>() {
            @Override
            public List<T> read(BitStream bs) {
                final ArrayList<T> xs = new ArrayList<>();
                while (bs.getBoolean()) {
                    xs.add(schema.read(bs));
                }
                return xs;
            }

            @Override
            public int maximumSizeBits() {
                return -1;
            }

            @Override
            public int sizeBits(List<T> xs) {
                int size = 0;
                for (T x : xs) {
                    size += schema.sizeBits(x);
                }
                return size;
            }

            @Override
            public void write(BitStream bs, List<T> xs) {
                for (T x : xs) {
                    bs.putBoolean(true);
                    schema.write(bs, x);
                }
                bs.putBoolean(false);
            }
        };
    }
}
