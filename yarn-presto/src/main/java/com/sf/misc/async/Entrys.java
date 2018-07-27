package com.sf.misc.async;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class Entrys {

    public static <K, V> Map.Entry<K, V> newEntry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    public static <K, V> Map.Entry<K, V> newImmutableEntry(K key, V value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }
}
