package uk.co.omegaprime.thunder.schema;

import java.time.LocalDate;

public class LocalDateSchema {
    public static Schema<LocalDate> INSTANCE = LongSchema.INSTANCE.map(LocalDate::toEpochDay, LocalDate::ofEpochDay);
}
