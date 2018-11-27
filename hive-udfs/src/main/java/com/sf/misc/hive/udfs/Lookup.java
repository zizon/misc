package com.sf.misc.hive.udfs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class Lookup extends GenericUDF {

    public static final Log LOGGER = LogFactory.getLog(Lookup.class);

    protected List<PrimitiveObjectInspectorConverter.StringConverter> argument_inspector;
    protected LoadingCache<String, Optional<List<Map<String, String>>>> source_caches = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build(new CacheLoader<String, Optional<List<Map<String, String>>>>() {
                @Override
                public Optional<List<Map<String, String>>> load(String source) throws Exception {
                    // dispatch the read load.
                    // given a source idnetity,transform it to a list map
                    try {
                        return loadSource(source);
                    } catch (Throwable throwable) {
                        //TODO: log it and should return an emtpy?
                        LOGGER.error("fail to load source", throwable);
                        return Optional.empty();
                    }
                }
            });
    protected Cache<String, Map<String, Map<String, String>>> lookup_cache = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build();

    abstract protected Optional<List<Map<String, String>>> loadSource(String source);

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        this.argument_inspector = Arrays.stream(arguments).parallel()
                .map((argument) -> new PrimitiveObjectInspectorConverter.StringConverter((PrimitiveObjectInspector) argument))
                .collect(Collectors.toList());
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public String getDisplayString(String[] children) {
        return this.getStandardDisplayString(this.getFuncName(), children, ",");
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // example:
        // lookup(source,index_key,match_value,target_field)
        // lookup('dim.dim_department','dept_name','SCL01R','division_code') -> CLSCLY
        final List<String> string_arguments = IntStream.range(0, arguments.length)
                .mapToObj((i) -> {
                    return this.argument_inspector.get(i).convert(arguments[i]).toString();
                })
                .collect(Collectors.toList());

        // prepare arguemtns
        final String source = string_arguments.get(0);
        final String index_key = string_arguments.get(1);
        final String match_value = string_arguments.get(2);
        final String target_filed = string_arguments.get(3);

        // find jsonify row
        return this.maybeBuildLookup(source, index_key)
                // find maps keyed by match_value
                .getOrDefault(match_value, Collections.emptyMap())
                // find target filed value if presented
                .get(target_filed);
    }

    protected Map<String, Map<String, String>> maybeBuildLookup(String source, String index_key) {
        final String cache_key = source + "|" + index_key;
        try {
            return lookup_cache.get(cache_key, () -> {
                // find rows from cache or load it
                return source_caches.get(source)
                        // with fallback empty rows
                        .orElseGet(() -> Collections.emptyList())
                        .parallelStream()
                        // then index by index key for fast lookup
                        .collect(Collectors.toConcurrentMap(
                                (row) -> row.get(index_key),
                                Function.identity()
                                )
                        );
            });
        } catch (ExecutionException e) {
            throw new RuntimeException("fail to fetch lookup rows, from source:" + source + " index_key:" + index_key);
        }
    }
}
