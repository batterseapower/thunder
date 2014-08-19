package uk.co.omegaprime.thunder.schema;

import uk.co.omegaprime.thunder.BitStream;
import uk.co.omegaprime.thunder.Pair;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

public interface Schema<T> {
    public static <T, U, V> Schema<V> zipWith(Schema<T> leftSchema, Function<V, T> leftProj, Schema<U> rightSchema, Function<V, U> rightProj, BiFunction<T, U, V> f) {
        return new Schema<V>() {
            @Override
            public V read(BitStream bs) {
                return f.apply(leftSchema.read(bs), rightSchema.read(bs));
            }

            @Override
            public int maximumSizeBits() {
                return (leftSchema.maximumSizeBits() >= 0 && rightSchema.maximumSizeBits() >= 0) ? Math.max(leftSchema.maximumSizeBits(), rightSchema.maximumSizeBits()) : -1;
            }

            @Override
            public int sizeBits(V x) {
                return leftSchema.sizeBits(leftProj.apply(x)) + rightSchema.sizeBits(rightProj.apply(x));
            }

            @Override
            public void write(BitStream bs, V x) {
                leftSchema.write (bs, leftProj .apply(x));
                rightSchema.write(bs, rightProj.apply(x));
            }
        };
    }

    public static <T, U> Schema<Pair<T, U>> zip(Schema<T> leftSchema, Schema<U> rightSchema) {
        return zipWith(leftSchema,  (Pair<T, U> pair) -> pair.k,
                       rightSchema, (Pair<T, U> pair) -> pair.v,
                       Pair::new);
    }

    public static <T> Schema<T> nullable(Schema<T> schema) {
        return optional(schema).map(Optional::ofNullable, (Optional<T> optional) -> optional.isPresent() ? optional.get() : null);
    }

    public static <T> Schema<Optional<T>> optional(Schema<T> schema) {
        return new Schema<Optional<T>>() {
            @Override
            public Optional<T> read(BitStream bs) {
                if (!bs.getBoolean()) {
                    return Optional.empty();
                } else {
                    return Optional.of(schema.read(bs));
                }
            }

            @Override
            public int maximumSizeBits() {
                return 2 + schema.maximumSizeBits();
            }

            @Override
            public int sizeBits(Optional<T> x) {
                return 1 + (x.isPresent() ? schema.sizeBits(x.get()) : 0);
            }

            @Override
            public void write(BitStream bs, Optional<T> x) {
                if (!x.isPresent()) {
                    bs.putBoolean(false);
                } else {
                    bs.putBoolean(true);
                    schema.write(bs, x.get());
                }
            }
        };
    }

    T read(BitStream bs);
    int maximumSizeBits();
    int sizeBits(T x);
    void write(BitStream bs, T x);

    default <U> Schema<U> map(Function<U, T> f, Function<T, U> g) {
        final Schema<T> parent = this;
        return new Schema<U>() {
            public U read(BitStream bs) {
                return g.apply(parent.read(bs));
            }

            public int maximumSizeBits() {
                return parent.maximumSizeBits();
            }

            public int sizeBits(U x) {
                return parent.sizeBits(f.apply(x));
            }

            public void write(BitStream bs, U x) {
                parent.write(bs, f.apply(x));
            }
        };
    }
}
