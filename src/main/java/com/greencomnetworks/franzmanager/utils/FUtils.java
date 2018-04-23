package com.greencomnetworks.franzmanager.utils;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Functional Utils
 *
<<<<<<< HEAD
 * Created by Loïc Gaillard.
=======
>>>>>>> github/master
 */
public class FUtils {

    public static <T> Optional<T> get(T val) {
        return Optional.ofNullable(val);
    }

    public static <T> T getOrElse(T val, T other) {
        if(val != null) return val;
        return other;
    }

    public static <T> T getOrElse(Callable<T> callable, T other) {
        try {
            return callable.call();
        } catch (Exception e) {
            return other;
        }
    }

    public static <T> T getOrElseGet(T val, Supplier<? extends T> otherSupplier) {
        if(val != null) return val;
        return otherSupplier.get();
    }

    public static <T, X extends Throwable> T getOrThrow(T val, Supplier<? extends X> throwableSupplier) throws X {
        if(val != null) return val;
        throw throwableSupplier.get();
    }


    public static <T, X extends Throwable> T getOrThrow(Callable<T> callable, Supplier<? extends X> throwableSupplier) throws X {
        try {
            return callable.call();
        } catch (Exception e) {
            throw throwableSupplier.get();
        }
    }

    public static <T, X extends Throwable> T getOrThrow(Callable<T> callable, Function<Exception, ? extends X> throwableFunction) throws X {
        try {
            return callable.call();
        } catch (Exception e) {
            throw throwableFunction.apply(e);
        }
    }

    public static ZonedDateTime toUTC(ZonedDateTime time) {
        if(time == null) return null;
        return time.withZoneSameInstant(ZoneOffset.UTC);
    }

    public static String toUTCString(ZonedDateTime time) {
        if(time == null) return null;
        return toUTC(time).toString();
    }

    public static class List {

        public static <T> java.util.List<T> empty() {
            return new ArrayList<>();
        }

        @SafeVarargs
        public static <T> java.util.List<T> of(T... elements) {
            java.util.List<T> list = new ArrayList<>(elements.length);
            Collections.addAll(list, elements);
            return list;
        }

        public static <T> java.util.List<T> from(Collection<? extends T> collection) {
            return new ArrayList<>(collection);
        }
    }

    public static class Set {

        public static <T> java.util.Set<T> empty() {
            return new HashSet<>();
        }

        @SafeVarargs
        public static <T> java.util.Set<T> of(T... elements) {
            java.util.Set<T> set = new HashSet<>(elements.length);
            Collections.addAll(set, elements);
            return set;
        }

        public static <T> java.util.Set<T> from(Collection<? extends T> collection) {
            return new HashSet<>(collection);
        }
    }

    public static class Map {

        public static <U, V> java.util.Map<U, V> empty() {
            return new HashMap<>();
        }

        public static <U, V> MapBuilder<U, V> builder() {
            return new MapBuilder<>();
        }

        public static <U, V> java.util.Map<U, V> of(U key, V val) {
            java.util.Map<U, V> map = new HashMap<>();
            map.put(key, val);
            return map;
        }

        @SuppressWarnings("unchecked")
        public static <U, V> java.util.Map<U, V> of(Object... fields) {
            if(fields.length % 2 != 0) throw new IllegalArgumentException(fields.length + " arguments provided, require an even number of arguments");
            java.util.Map<U, V> builder = new HashMap<>();
            for (int i = 0; i < fields.length; i += 2) {
                builder.put((U) fields[i], (V) fields[i+1]);
            }
            return builder;
        }

        @SafeVarargs
        public static <U, V> java.util.Map<U, V> of(java.util.Map.Entry<U, V>... entries) {
            java.util.Map<U, V> builder = new HashMap<>();
            for (java.util.Map.Entry<U, V> entry : entries) {
                builder.put(entry.getKey(), entry.getValue());
            }
            return builder;
        }

        public static <U, V> java.util.Map<U, V> from(java.util.Map<? extends U, ? extends V> map) {
            return new HashMap<>(map);
        }


        public static class MapBuilder<U, V> {
            private java.util.Map<U, V> map = new HashMap<>();

            public MapBuilder<U, V> put(U key, V val) {
                map.put(key, val);
                return this;
            }

            public MapBuilder<U, V> putAll(java.util.Map<? extends U, ? extends V> entries) {
                map.putAll(entries);
                return this;
            }

            public MapBuilder<U, V> filter(Predicate<? super java.util.Map.Entry<U, V>> predicate) {
                map = map.entrySet().stream()
                        .filter(predicate)
                        .collect(Collectors.toMap(
                                java.util.Map.Entry::getKey,
                                java.util.Map.Entry::getValue));

                return this;
            }

            public java.util.Map<U, V> build() {
                return map;
            }
        }
    }

    public static class Stream {

        public static <T> java.util.stream.Stream<T> empty() {
            return java.util.stream.Stream.empty();
        }
    }
}
