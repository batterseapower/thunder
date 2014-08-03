package uk.co.omegaprime.thunder.schema;

import java.nio.charset.Charset;

public class StringSchema {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    // XXX: due to the structure of UTF-8 it's actually possible to have less overhead than this
    public static Schema<String> INSTANCE = ByteArraySchema.INSTANCE.map((String x) -> x.getBytes(UTF8), (byte[] xs) -> new String(xs, UTF8));
}
