package uk.co.omegaprime.thunder;

import java.util.Objects;

public final class Pair<K, V> {
    public final K k;
    public final V v;

    public Pair(K k, V v) {
        this.k = k;
        this.v = v;
    }

    public boolean equals(Object thatObject) {
        if (!(thatObject instanceof Pair)) {
            return false;
        } else {
            Pair that = (Pair)thatObject;
            return Objects.equals(this.k, that.k) && Objects.equals(this.v, that.v);
        }
    }

    @Override
    public String toString() {
        return "Pair(" + k + ", " + v + ")";
    }
}
