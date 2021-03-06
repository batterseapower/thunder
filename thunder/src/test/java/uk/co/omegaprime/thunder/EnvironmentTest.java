package uk.co.omegaprime.thunder;

import org.junit.Test;
import uk.co.omegaprime.thunder.schema.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.omegaprime.thunder.Bits.*;

public class EnvironmentTest {
    private static Supplier<Environment> prepareEnvironment() {
        final File envDirectory;
        try {
            envDirectory = Files.createTempDirectory("DatabaseTest").toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final File[] files = envDirectory.listFiles();
        if (files != null) {
            for (File f : files) {
                f.delete();
            }
            if (!envDirectory.delete()) {
                throw new IllegalStateException("Failed to delete target directory " + envDirectory);
            }
        }
        envDirectory.mkdir();
        envDirectory.deleteOnExit();

        return () -> new Environment(envDirectory, new EnvironmentOptions().maxDatabases(40).mapSize(1024 * 1024));
    }

    private static Environment createEnvironment() {
        return prepareEnvironment().get();
    }

    @Test
    public void canCursorAroundPositiveFloatsAndNaNs() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Float, String> database = env.createDatabase(tx, "Test", FloatSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, 1.0f, "One");
                database.put(tx, 1234567890123.0f, "Biggish");
                database.put(tx, Float.NaN, "Not a number");
                database.put(tx, Float.POSITIVE_INFINITY, "Infinity");

                assertEquals("One", database.get(tx, 1.0f));
                assertEquals("Biggish", database.get(tx, 1234567890123.0f));
                assertEquals("Not a number", database.get(tx, Float.NaN));
                assertEquals("Infinity", database.get(tx, Float.POSITIVE_INFINITY));

                try (Cursor<Float, String> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveCeiling(0.5f));
                    assertEquals("One", cursor.getValue());

                    assertFalse(cursor.movePrevious());

                    assertTrue(cursor.moveCeiling(100f));
                    assertEquals("Biggish", cursor.getValue());

                    assertTrue(cursor.movePrevious());
                    assertEquals("One", cursor.getValue());

                    assertTrue(cursor.moveCeiling(12345678901234.0f));
                    assertEquals("Infinity", cursor.getValue());

                    assertTrue(cursor.moveNext());
                    assertEquals("Not a number", cursor.getValue());

                    assertTrue(cursor.moveFloor(Float.NaN));
                    assertEquals("Not a number", cursor.getValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreLocalDates() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<LocalDate, String> database = env.createDatabase(tx, "Test", LocalDateSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, LocalDate.of(1999, 1, 1), "One");
                database.put(tx, LocalDate.of(1999, 1, 3), "Two");
                database.put(tx, LocalDate.of(1999, 1, 5), "Three");

                try (Cursor<LocalDate, String> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveCeiling(LocalDate.of(1998, 1, 1)));
                    assertEquals(LocalDate.of(1999, 1, 1), cursor.getKey());
                    assertEquals("One", cursor.getValue());

                    assertTrue(cursor.moveCeiling(LocalDate.of(1999, 1, 2)));
                    assertEquals(LocalDate.of(1999, 1, 3), cursor.getKey());
                    assertEquals("Two", cursor.getValue());

                    assertTrue(cursor.moveLast());
                    assertEquals(LocalDate.of(1999, 1, 5), cursor.getKey());
                    assertEquals("Three", cursor.getValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void canPutWithJustAValueIntoCursor() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 2, 200);
                database.put(tx, 3, 300);
                database.put(tx, 5, 400);

                try (Cursor<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveTo(3));
                    assertEquals(300, cursor.getValue().intValue());
                    cursor.put(301);
                    assertEquals(301, cursor.getValue().intValue());

                    assertTrue(cursor.movePrevious());
                    assertTrue(cursor.moveNext());
                    assertEquals(301, cursor.getValue().intValue());

                    cursor.put(4, 400);
                    cursor.put(401);
                    assertEquals(401, cursor.getValue().intValue());

                    assertTrue(cursor.movePrevious());
                    assertTrue(cursor.moveNext());
                    assertEquals(401, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void canRemove() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 3, 300);

                assertEquals(Arrays.asList(1, 2, 3), iteratorToList(database.keys(tx)));

                assertTrue(database.remove(tx, 2));
                assertEquals(Arrays.asList(1, 3), iteratorToList(database.keys(tx)));

                assertFalse(database.remove(tx, 2));
                assertEquals(Arrays.asList(1, 3), iteratorToList(database.keys(tx)));

                assertTrue(database.remove(tx, 3));
                assertEquals(Arrays.asList(1), iteratorToList(database.keys(tx)));
            }
        }
    }

    @Test
    public void canGetValues() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 3, 100);

                assertEquals(Arrays.asList(100, 200, 100), iteratorToList(database.values(tx)));
            }
        }
    }

    @Test
    public void canGetKeyValues() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 3, 100);

                assertEquals(Arrays.asList(new Pair<>(1, 100), new Pair<>(2, 200), new Pair<>(3, 100)),
                             iteratorToList(database.keyValues(tx)));
            }
        }
    }

    @Test
    public void deletionLeavesCursorPointingAtNextItem() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 3, 100);

                try (final Cursor<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));

                    cursor.delete();
                    assertTrue(cursor.isPositioned());
                    assertEquals(Arrays.asList(1, 3), iteratorToList(database.keys(tx)));

                    cursor.delete();
                    assertFalse(cursor.isPositioned());
                    assertEquals(Arrays.asList(1), iteratorToList(database.keys(tx)));

                    assertTrue(cursor.moveFirst());
                    cursor.delete();
                    assertFalse(cursor.isPositioned());
                    assertFalse(database.keys(tx).hasNext());
                }
            }
        }
    }

    @Test
    public void deletionLeavesCursorPointingAtNextItemOfDuplicate() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 0, 50);
                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 2, 300);
                database.put(tx, 3, 400);
                database.put(tx, 3, 500);

                try (final CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));

                    cursor.delete();
                    assertTrue(cursor.isPositioned());
                    assertEquals(Arrays.asList(0, 1, 2, 3), iteratorToList(database.keys(tx)));
                    assertEquals(300, cursor.getValue().intValue());

                    cursor.delete();
                    assertTrue(cursor.isPositioned());
                    assertEquals(Arrays.asList(0, 1, 3), iteratorToList(database.keys(tx)));
                    assertEquals(400, cursor.getValue().intValue());

                    cursor.deleteAllOfKey();
                    assertFalse(cursor.isPositioned());
                    assertEquals(Arrays.asList(0, 1), iteratorToList(database.keys(tx)));

                    assertTrue(cursor.moveFirst());
                    cursor.deleteAllOfKey();
                    assertTrue(cursor.isPositioned());
                    assertEquals(Arrays.asList(1), iteratorToList(database.keys(tx)));
                    assertEquals(100, cursor.getValue().intValue());

                    assertTrue(cursor.moveFirst());
                    cursor.deleteAllOfKey();
                    assertFalse(cursor.isPositioned());
                    assertFalse(database.keys(tx).hasNext());
                }
            }
        }
    }

    @Test
    public void canCursorAroundDatabaseWithDuplicateKeys() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 2, 200);
                database.put(tx, 2, 300);
                database.put(tx, 2, 200);
                database.put(tx, 4, 400);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveFirst());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(1, cursor.keyItemCount());

                    assertFalse(cursor.moveNextOfKey());
                    assertFalse(cursor.movePreviousOfKey());

                    assertTrue(cursor.moveNext());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(2, cursor.keyItemCount());

                    assertTrue(cursor.moveNext());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveNext());
                    assertEquals(4, cursor.getKey().intValue());
                    assertEquals(400, cursor.getValue().intValue());

                    assertTrue(cursor.moveLastOfPreviousKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveFirstOfNextKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    assertTrue(cursor.moveLastOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertFalse(cursor.moveNextOfKey());
                    assertTrue(cursor.movePreviousOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    assertFalse(cursor.movePreviousOfKey());
                    assertTrue(cursor.moveNextOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveFirstOfKey());
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());

                    // This exposed a bug in LMDB: the *{First,Last}OfKey methods didn't work if you were pointing to an entry without duplicates
                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveLastOfKey());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void canCursorSeekIntoDatabaseWithDuplicateKeys() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 2, 300);
                database.put(tx, 4, 400);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveFloor(3));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeiling(3));
                    assertEquals(400, cursor.getValue().intValue());

                    // The two-arg versions of moveFloorOfKey/moveCeilingOfKey should never move us to a different K value than the one we asked for
                    assertFalse(cursor.moveFloorOfKey(3, 350));
                    assertFalse(cursor.moveCeilingOfKey(3, 350));

                    assertFalse(cursor.moveFloorOfKey(2, 150));
                    assertTrue(cursor.moveFloorOfKey(2, 250));
                    assertEquals(200, cursor.getValue().intValue());

                    assertTrue(cursor.moveFloorOfKey(2, 350));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeilingOfKey(2, 250));
                    assertEquals(300, cursor.getValue().intValue());

                    assertTrue(cursor.moveCeilingOfKey(2, 150));
                    assertEquals(200, cursor.getValue().intValue());

                    assertFalse(cursor.moveCeilingOfKey(2, 350));
                    assertFalse(cursor.moveTo(1, 101));
                    assertFalse(cursor.moveTo(2, 201));
                    assertFalse(cursor.moveTo(2, 199));
                    assertTrue(cursor.moveTo(2, 200));

                    cursor.delete();
                    assertTrue(cursor.isPositioned());
                    assertFalse(cursor.moveTo(2, 200));
                    assertTrue(cursor.moveTo(2, 300));
                }
            }
        }
    }

    @Test
    public void canPutWithJustAValueIntoCursorWithDuplicateKeys() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 2, 200);
                database.put(tx, 2, 200);
                database.put(tx, 2, 300);
                database.put(tx, 2, 300);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));
                    assertEquals(2, cursor.keyItemCount());

                    cursor.put(200);
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(2, cursor.keyItemCount());

                    cursor.put(250);
                    assertEquals(2, cursor.getKey().intValue());
                    assertEquals(250, cursor.getValue().intValue());
                    assertEquals(3, cursor.keyItemCount());
                }
            }
        }
    }

    @Test
    public void databaseLevelOperationsWorkOnDatabaseWithDuplicateKeys() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 2, 200);
                database.put(tx, 2, 200);
                database.put(tx, 2, 300);
                database.put(tx, 2, 200);
                database.put(tx, 4, 400);

                assertTrue(database.contains(tx, 1, 100));
                assertFalse(database.contains(tx, 1, 150));
                assertTrue(database.contains(tx, 2, 200));
                assertFalse(database.contains(tx, 2, 250));
                assertTrue(database.contains(tx, 2, 300));

                assertTrue(database.remove(tx, 2, 200));
                assertFalse(database.contains(tx, 2, 200));
                assertTrue(database.contains(tx, 2, 300));

                assertFalse(database.remove(tx, 2, 250));

                assertTrue(database.remove(tx, 1, 100));
                assertFalse(database.contains(tx, 1, 100));

                database.put(tx, 2, 200);
                assertTrue(database.remove(tx, 2));
                assertFalse(database.contains(tx, 2, 200));
                assertFalse(database.contains(tx, 2, 300));

                assertFalse(database.remove(tx, 2));
            }
        }
    }

    @Test
    public void putIfAbsentShouldWorkOnNormalDatabase() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, Integer> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                assertEquals(null, database.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), database.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), database.putIfAbsent(tx, 1, 200));
                assertEquals(Integer.valueOf(100), database.putIfAbsent(tx, 1, 100));
                assertEquals(null, database.putIfAbsent(tx, 2, 100));

                try (Cursor<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertEquals(Integer.valueOf(100), cursor.putIfAbsent(2, 200));
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(3, 100));
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void putIfAbsentShouldWorkOnDatabaseWithDuplicates() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                assertEquals(null, database.putIfAbsent(tx, 1, 100));
                assertEquals(Integer.valueOf(100), database.putIfAbsent(tx, 1, 100));
                assertEquals(null, database.putIfAbsent(tx, 1, 200));
                assertEquals(Integer.valueOf(100), database.putIfAbsent(tx, 1, 100));
                assertEquals(null, database.putIfAbsent(tx, 2, 100));

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertEquals(Integer.valueOf(100), cursor.putIfAbsent(2, 100));
                    assertEquals(100, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(2, 200));
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(Integer.valueOf(200), cursor.putIfAbsent(2, 200));
                    assertEquals(200, cursor.getValue().intValue());
                    assertEquals(null, cursor.putIfAbsent(3, 100));
                    assertEquals(100, cursor.getValue().intValue());
                }
            }
        }
    }

    @Test
    public void deleteAllOfKeyWorks() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final DatabaseWithDuplicateKeys<Integer, Integer> database = env.createDatabaseWithDuplicateKeys(tx, "Test", IntegerSchema.INSTANCE, IntegerSchema.INSTANCE);

                database.put(tx, 1, 100);
                database.put(tx, 1, 200);
                database.put(tx, 2, 100);
                database.put(tx, 3, 200);

                try (CursorWithDuplicateKeys<Integer, Integer> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveFirst());
                    assertEquals(2, cursor.keyItemCount());

                    cursor.deleteAllOfKey();
                    assertTrue(cursor.moveFirst());
                    assertEquals(Integer.valueOf(2), cursor.getKey());
                    assertEquals(1, cursor.keyItemCount());

                    cursor.deleteAllOfKey();
                    assertTrue(cursor.moveFirst());
                    assertEquals(Integer.valueOf(3), cursor.getKey());
                }
            }
        }
    }

    private static <T> List<T> iteratorToList(Iterator<T> it) {
        final ArrayList<T> result = new ArrayList<>();
        while (it.hasNext()) {
            result.add(it.next());
        }
        return result;
    }

    @Test
    public void canStoreNullFreeStrings() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<String, Long> database = env.createDatabase(tx, "Test", NullFreeStringSchema.INSTANCE, LongSchema.INSTANCE);

                database.put(tx, "Foo", 1l);
                database.put(tx, "Fooa", 2l);
                database.put(tx, "Fo", 3l);
                database.put(tx, "Foa", 4l);

                assertEquals(Arrays.asList("Fo", "Foa", "Foo", "Fooa"), iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreStrings() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<String, Long> database = env.createDatabase(tx, "Test", StringSchema.INSTANCE, LongSchema.INSTANCE);

                database.put(tx, "Foo", 1l);
                database.put(tx, "Fooa", 2l);
                database.put(tx, "Fo", 3l);
                database.put(tx, "Foa", 4l);

                assertEquals(Arrays.asList("Fo", "Foa", "Foo", "Fooa"), iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreCompositeStringKey() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<String, Integer> database = env.createDatabase(tx, "Test", Schema.zipWith(StringSchema.INSTANCE, (String x) -> x.split("/", 2)[0],
                                StringSchema.INSTANCE, (String x) -> x.split("/", 2)[1],
                                (String x, String y) -> x + "/" + y),
                        IntegerSchema.INSTANCE);

                database.put(tx, "Food/Bean", 10);
                database.put(tx, "Air/Bean", 11);
                database.put(tx, "Apple/Bean", 12);
                database.put(tx, "Apple/Beans", 13);
                database.put(tx, "Apple/Carrot", 14);
                database.put(tx, "Airpie/Bean", 15);

                assertEquals(Arrays.asList("Air/Bean", "Airpie/Bean", "Apple/Bean", "Apple/Beans", "Apple/Carrot", "Food/Bean"),
                             iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreLongs() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Long, String> database = env.createDatabase(tx, "Test", LongSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, -100000000l, "Neg Big");
                database.put(tx, -1l, "Neg One");
                database.put(tx, 1l, "One");
                database.put(tx, 100000000l, "Big");

                assertEquals(Arrays.asList(-100000000l, -1l, 1l, 100000000l), iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreFloats() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Float, String> database = env.createDatabase(tx, "Test", FloatSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, Float.NEGATIVE_INFINITY, "Neg Inf");
                database.put(tx, -100000000.0f, "Neg Big");
                database.put(tx, -1.0f, "Neg One");
                database.put(tx, 1.0f, "One");
                database.put(tx, 100000000.0f, "Big");
                database.put(tx, Float.POSITIVE_INFINITY, "Neg Inf");

                assertEquals(Arrays.asList(Float.NEGATIVE_INFINITY, -100000000.0f, -1.0f, 1.0f, 100000000.0f, Float.POSITIVE_INFINITY), iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void canStoreDoubles() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Double, String> database = env.createDatabase(tx, "Test", DoubleSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, Double.NEGATIVE_INFINITY, "Neg Inf");
                database.put(tx, -100000000.0, "Neg Big");
                database.put(tx, -1.0, "Neg One");
                database.put(tx, 1.0, "One");
                database.put(tx, 100000000.0, "Big");
                database.put(tx, Double.POSITIVE_INFINITY, "Neg Inf");

                assertEquals(Arrays.asList(Double.NEGATIVE_INFINITY, -100000000.0, -1.0, 1.0, 100000000.0, Double.POSITIVE_INFINITY), iteratorToList(database.keys(tx)));

                tx.commit();
            }
        }
    }

    @Test
    public void unsignedIntegerSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Integer.BYTES);
        try {
            UnsignedIntegerSchema.INSTANCE.write(new BitStream(ptr, Integer.BYTES), 0xCAFEBABE);
            assertEquals((byte)0xCA, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xBE, unsafe.getByte(ptr + Integer.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void integerSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Integer.BYTES);
        try {
            IntegerSchema.INSTANCE.write(new BitStream(ptr, Integer.BYTES), 0xCAFEBABE);
            // 0xC = 12 = 1010b ==> 0010b = 0x4
            assertEquals((byte)0x4A, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xBE, unsafe.getByte(ptr + Integer.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void unsignedLongSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Long.BYTES);
        try {
            UnsignedLongSchema.INSTANCE.write(new BitStream(ptr, Long.BYTES), 0xCAFEBABEDEADBEEFl);
            assertEquals((byte)0xCA, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xEF, unsafe.getByte(ptr + Long.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void longSchemaStoresCorrectOrdering() {
        final long ptr = unsafe.allocateMemory(Long.BYTES);
        try {
            LongSchema.INSTANCE.write(new BitStream(ptr, Long.BYTES), 0xCAFEBABEDEADBEEFl);
            // 0xC = 12 = 1010b ==> 0010b = 0x4
            assertEquals((byte)0x4A, unsafe.getByte(ptr + 0));
            assertEquals((byte)0xFE, unsafe.getByte(ptr + 1));
            assertEquals((byte)0xEF, unsafe.getByte(ptr + Long.BYTES - 1));
        } finally {
            unsafe.freeMemory(ptr);
        }
    }

    @Test
    public void singleTransaction() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, String> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, 1, "Hello");
                database.put(tx, 2, "World");
                database.put(tx, 3, "!");

                assertEquals("World", database.get(tx, 2));

                tx.commit();
            }
        }
    }

    @Test
    public void doubleTransaction() {
        try (final Environment env = createEnvironment()) {
            final Database<Integer, String> database;
            try (final Transaction tx = env.transaction(false)) {
                database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, 1, "Hello");
                database.put(tx, 2, "World");
                database.put(tx, 3, "!");

                tx.commit();
            }

            try (final Transaction tx = env.transaction(true)) {
                assertEquals("World", database.get(tx, 2));
            }
        }
    }

    @Test
    public void doubleDatabase() {
        final Supplier<Environment> dbSupplier = prepareEnvironment();
        try (final Environment env = dbSupplier.get()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, String> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                database.put(tx, 1, "Hello");
                database.put(tx, 2, "World");
                database.put(tx, 3, "!");

                tx.commit();
            }
        }

        try (final Environment env = dbSupplier.get()) {
            try (final Transaction tx = env.transaction(true)) {
                final Database<Integer, String> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);

                assertEquals("World", database.get(tx, 2));
            }
        }
    }

    @Test
    public void singleCursoredTransaction() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, String> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);
                try (final Cursor<Integer, String> cursor = database.createCursor(tx)) {
                    cursor.put(1, "Hello");
                    cursor.put(2, "World");
                    cursor.put(3, "!");

                    assertTrue(cursor.moveTo(2));
                    assertEquals("World", cursor.getValue());
                    assertEquals(2, cursor.getKey().longValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void doubleCursoredTransaction() {
        try (final Environment env = createEnvironment()) {
            final Database<Integer, String> database;
            try (final Transaction tx = env.transaction(false)) {
                database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, StringSchema.INSTANCE);
                try (final Cursor<Integer, String> cursor = database.createCursor(tx)) {
                    cursor.put(1, "Hello");
                    cursor.put(2, "World");
                    cursor.put(3, "!");
                }

                tx.commit();
            }

            try (final Transaction tx = env.transaction(true)) {
                try (final Cursor<Integer, String> cursor = database.createCursor(tx)) {
                    assertTrue(cursor.moveTo(2));
                    assertEquals("World", cursor.getValue());
                    assertEquals(2, cursor.getKey().longValue());
                }
            }
        }
    }

    @Test
    public void boundedSizeKeyValues() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<String, String> database = env.createDatabase(tx, "Test", new Latin1StringSchema(10), new Latin1StringSchema(10));

                database.put(tx, "Hello", "World");
                database.put(tx, "Goodbye", "Hades");

                assertEquals("World", database.get(tx, "Hello"));

                tx.commit();
            }
        }
    }

    @Test
    public void moveCursor() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Integer, String> database = env.createDatabase(tx, "Test", IntegerSchema.INSTANCE, new Latin1StringSchema(10));

                database.put(tx, 1, "World");
                database.put(tx, 3, "Heaven");
                database.put(tx, 5, "Hades");

                try (final Cursor<Integer, String> cursor = database.createCursor(tx)) {

                    // moveFirst/moveNext/moveLast

                    assertTrue(cursor.moveFirst());
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertFalse(cursor.movePrevious());

                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveLast());
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveNext());


                    // movePrevious when you are at the start doesn't do anything to your current position:
                    assertTrue(cursor.moveFirst());
                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());

                    // moveNext when you are at the end doesn't do anything to your current position:
                    assertTrue(cursor.moveLast());
                    assertTrue(cursor.movePrevious());
                    assertEquals(3, cursor.getKey().intValue());


                    // moveTo

                    assertTrue(cursor.moveTo(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveTo(1));
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertTrue(cursor.moveTo(5));
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveTo(4));


                    // moveCeilingOfKey

                    assertTrue(cursor.moveCeiling(2));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveCeiling(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveCeiling(0));
                    assertEquals(1, cursor.getKey().intValue());
                    assertEquals("World", cursor.getValue());

                    assertFalse(cursor.moveCeiling(6));

                    // At this point the cursor is "off the end" so going back 1 will take us to the last item
                    assertTrue(cursor.movePrevious());
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());


                    // moveFloorOfKey

                    assertTrue(cursor.moveFloor(4));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveFloor(3));
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());

                    assertTrue(cursor.moveFloor(6));
                    assertEquals(5, cursor.getKey().intValue());
                    assertEquals("Hades", cursor.getValue());

                    assertFalse(cursor.moveFloor(0));

                    // At this point the cursor is (a bit inconsistently..) actually pointing to the first item.
                    // There doesn't seem be any way to get it to go "off the beginning" such that moveNext()
                    // takes you to the first item, otherwise I'd probably arrange for moveFloorOfKey() to do that.
                    // (I tried going to the first item then doing movePrevious() but that just leaves you in the same place.)
                    assertTrue(cursor.moveNext());
                    assertEquals(3, cursor.getKey().intValue());
                    assertEquals("Heaven", cursor.getValue());
                }

                tx.commit();
            }
        }
    }

    @Test
    public void moveFloorWorksInParticularBuggyCase() {
        // There was a bug where keyEquals would not compare the final 1 to 7 bits of the key, so moveFloor would erroneously return true
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Pair<String, Integer>, String> database = env.createDatabase(tx, "Test",
                        Schema.zip(new Latin1StringSchema(20), IntegerSchema.INSTANCE), new Latin1StringSchema(10));

                database.put(tx, new Pair<>("foo", 5), "First0");

                try (final Cursor<Pair<String, Integer>, String> cursor = database.createCursor(tx)) {
                    assertFalse(cursor.moveFloor(new Pair<>("foo", 4)));
                }
            }
        }
    }

    @Test
    public void subcursorWorks() {
        try (final Environment env = createEnvironment()) {
            try (final Transaction tx = env.transaction(false)) {
                final Database<Pair<Integer, Integer>, String> database = env.createDatabase(tx, "Test",
                        Schema.zip(UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE), new Latin1StringSchema(10));

                database.put(tx, new Pair<>(0, 0), "First0");
                database.put(tx, new Pair<>(0, 2), "First1");
                database.put(tx, new Pair<>(100, 0), "Middle0");
                database.put(tx, new Pair<>(100, 2), "Middle1");
                database.put(tx, new Pair<>(200, 100), "Singleton");
                database.put(tx, new Pair<>(0xFFFFFFFF, 0), "Last0");
                database.put(tx, new Pair<>(0xFFFFFFFF, 2), "Last0");

                try (final Cursor<Pair<Integer, Integer>, String> cursor = database.createCursor(tx)) {
                    for (int a : new int[] { 0, 100, 0xFFFFFFFF }) {
                        final Cursorlike<Integer, String> subcursor = new SubcursorView<>(cursor, UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE, a);

                        assertTrue(subcursor.moveFirst());
                        assertEquals(0, subcursor.getKey().intValue());
                        assertTrue(subcursor.moveNext());
                        assertEquals(2, subcursor.getKey().intValue());
                        assertFalse(subcursor.moveNext());

                        assertTrue(subcursor.moveLast());
                        assertEquals(2, subcursor.getKey().intValue());
                        assertTrue(subcursor.movePrevious());
                        assertEquals(0, subcursor.getKey().intValue());
                        assertFalse(subcursor.movePrevious());

                        assertTrue(subcursor.moveFloor(1));
                        assertEquals(0, subcursor.getKey().intValue());
                        assertTrue(subcursor.moveFloor(2));
                        assertEquals(2, subcursor.getKey().intValue());

                        assertTrue(subcursor.moveCeiling(0));
                        assertEquals(0, subcursor.getKey().intValue());
                        assertTrue(subcursor.moveCeiling(1));
                        assertEquals(2, subcursor.getKey().intValue());

                        assertEquals(new Pair<>(a, 2), cursor.getKey());
                    }

                    {
                        final Cursorlike<Integer, String> subcursor = new SubcursorView<>(cursor, UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE, 1337);
                        assertFalse(subcursor.moveFirst());
                        assertFalse(subcursor.moveLast());
                        assertFalse(subcursor.moveTo(100));
                        assertFalse(subcursor.moveCeiling(100));
                        assertFalse(subcursor.moveFloor(100));
                    }

                    {
                        final Cursorlike<Integer, String> subcursor = new SubcursorView<>(cursor, UnsignedIntegerSchema.INSTANCE, UnsignedIntegerSchema.INSTANCE, 200);
                        assertTrue(subcursor.moveFirst());
                        assertEquals(100, subcursor.getKey().intValue());
                        assertFalse(subcursor.moveNext());

                        assertTrue(subcursor.moveLast());
                        assertEquals(100, subcursor.getKey().intValue());
                        assertFalse(subcursor.movePrevious());

                        assertFalse(subcursor.moveTo(101));
                        assertTrue(subcursor.moveTo(100));

                        assertEquals(new Pair<>(200, 100), cursor.getKey());

                        assertFalse(subcursor.moveFloor(99));
                        assertTrue(subcursor.moveFloor(101));

                        assertFalse(subcursor.moveCeiling(101));
                        assertTrue(subcursor.moveCeiling(99));
                    }
                }
            }
        }
    }
}
