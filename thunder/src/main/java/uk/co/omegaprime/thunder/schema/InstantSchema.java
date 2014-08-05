package uk.co.omegaprime.thunder.schema;

import java.time.Instant;

public class InstantSchema {
    public static Schema<Instant> INSTANCE_SECOND_RESOLUTION = LongSchema.INSTANCE.map(Instant::getEpochSecond, Instant::ofEpochSecond);
}
