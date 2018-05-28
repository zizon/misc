package com.sf.misc.presto;

import com.facebook.presto.client.ClientSession;
import com.facebook.presto.client.Column;
import com.facebook.presto.client.OkHttpUtil;
import com.facebook.presto.client.QueryData;
import com.facebook.presto.client.StatementClient;
import com.facebook.presto.client.StatementClientFactory;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.spi.security.Identity;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.sf.misc.async.ExecutorServices;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.security.Principal;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

public class SessionBuilder {

    public static final Logger LOGGER = Logger.get(SessionBuilder.class);


    protected static LoadingCache<Identity, OkHttpClient> CLIENT_CACHE = CacheBuilder.newBuilder() //
            .expireAfterAccess(1, TimeUnit.MINUTES) //
            .build(new CacheLoader<Identity, OkHttpClient>() {
                OkHttpClient template = new OkHttpClient.Builder().readTimeout(10, TimeUnit.SECONDS).build();

                @Override
                public OkHttpClient load(Identity key) throws Exception {
                    return template.newBuilder().addInterceptor( //
                            OkHttpUtil.basicAuth(//
                                    key.getUser(),
                                    ((TokenPrincipal) key.getPrincipal().get()).getToken()) //
                    ).build();
                }
            });

    public static class PrestoSession {
        protected ClientSession session;
        protected Identity identity;

        protected PrestoSession(ClientSession session, Identity identity) {
            this.session = session;
            this.identity = identity;
        }

        protected ListenableFuture<Iterator<List<Map.Entry<Column, Object>>>> query(String query, Consumer<StatementStats> stats) {
            SettableFuture<Iterator<QueryData>> result = SettableFuture.create();

            ListenableFuture<StatementClient> statement = ExecutorServices.executor().submit(() -> {
                return StatementClientFactory.newStatementClient(CLIENT_CACHE.get(identity), session, query);
            });

            return Futures.transform(statement, (client) -> {
                return StreamSupport.stream(new Iterable<Iterator<List<Map.Entry<Column, Object>>>>() {
                    @Override
                    public Iterator<Iterator<List<Map.Entry<Column, Object>>>> iterator() {
                        return new Iterator<Iterator<List<Map.Entry<Column, Object>>>>() {
                            @Override
                            public boolean hasNext() {
                                try {
                                    stats.accept(client.getStats());
                                } catch (Throwable throwable) {
                                    LOGGER.warn(throwable, "unexpected exception when reporting query stat");
                                }
                                return client.advance();
                            }

                            @Override
                            public Iterator<List<Map.Entry<Column, Object>>> next() {
                                Iterable<List<Object>> iterable = client.currentData().getData();
                                if (iterable == null) {
                                    return null;
                                }

                                Iterator<List<Object>> iterator = iterable.iterator();
                                List<Column> columns = client.currentStatusInfo().getColumns();

                                return StreamSupport.stream(iterable.spliterator(), false).map((row) -> {
                                    return IntStream.range(0, columns.size()).mapToObj((i) -> {
                                        return (Map.Entry<Column, Object>) new AbstractMap.SimpleImmutableEntry<Column, Object>(columns.get(i), row.get(i));
                                    }).collect(Collectors.toList());
                                }).iterator();
                            }
                        };
                    }
                }.spliterator(), false) //
                        .filter(Predicates.notNull()) //
                        .flatMap((iterator) -> {
                            return StreamSupport.stream(new Iterable<List<Map.Entry<Column, Object>>>() {
                                @Override
                                public Iterator<List<Map.Entry<Column, Object>>> iterator() {
                                    return iterator;
                                }
                            }.spliterator(), false);
                        }).iterator();
            });
        }
    }

    public static class TokenPrincipal implements Principal {


        protected final String token;

        public TokenPrincipal(String token) {
            this.token = token;
        }

        public String getToken() {
            return token;
        }

        @Override
        public String getName() {
            return this.getClass().getSimpleName();
        }
    }


    protected String user;
    protected URI coordinator;
    protected String user_agent;
    protected String catalog;
    protected Duration timeout;
    protected String schema;
    protected Principal token;

    public SessionBuilder doAs(String user) {
        this.user = user;
        return this;
    }

    public SessionBuilder coordinator(URI coordinator) {
        this.coordinator = coordinator;
        return this;
    }

    public SessionBuilder userAgent(String user_agent) {
        this.user_agent = user_agent;
        return this;
    }

    public SessionBuilder catalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public SessionBuilder timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    public SessionBuilder schema(String schema) {
        this.schema = schema;
        return this;
    }

    public SessionBuilder token(String token) {
        this.token = new TokenPrincipal(token);
        return this;
    }

    public PrestoSession build() {
        Identity identity = new Identity(Optional.of(user).get(), Optional.of(token));
        return new PrestoSession( //
                new ClientSession(
                        Optional.of(coordinator).get(),
                        identity.getUser(), //
                        Optional.ofNullable(user_agent).orElse("generated-presto-session-client"), //
                        Collections.emptySet(),
                        null,
                        Optional.ofNullable(catalog).orElse("hive"),
                        Optional.ofNullable(schema).orElse("default"),
                        TimeZone.getDefault().getID(),
                        Locale.getDefault(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        "",
                        Optional.ofNullable(timeout).orElse(new Duration(60, TimeUnit.SECONDS))
                ), //
                identity
        );
    }
}
