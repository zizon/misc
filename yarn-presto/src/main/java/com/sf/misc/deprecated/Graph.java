package com.sf.misc.deprecated;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.sf.misc.async.ListenablePromise;
import com.sf.misc.async.Promises;
import com.sf.misc.async.SettablePromise;
import io.airlift.log.Logger;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Graph<Payload> {
    public static final Logger LOGGER = Logger.get(Graph.class);

    public class Vertex {

        protected Optional<Payload> payload;

        protected String name;

        protected Set<String> outwards;

        public Vertex(String name, Payload payload) {
            this.payload = Optional.ofNullable(payload);
            this.name = name;
            this.outwards = Sets.newConcurrentHashSet();
        }

        public Optional<Payload> getPayload() {
            return payload;
        }

        public String getName() {
            return name;
        }

        public Graph<Payload> graph() {
            return Graph.this;
        }

        public Vertex link(String vertex_name) {
            this.outwards.add(vertex_name);
            return this;
        }

        public Vertex unlink(String vertext_name) {
            this.outwards.remove(vertext_name);
            return this;
        }

        public Stream<Graph<Payload>.Vertex> outwards() {
            return this.outwards.parallelStream().map((name) -> graph().vertex(name));
        }

        public Stream<String> outwardNames() {
            return this.outwards.parallelStream();
        }

        private Vertex bind(Payload payload) {
            this.payload = Optional.ofNullable(payload);
            return this;
        }

        @Override
        public String toString() {
            return new StringBuilder().append(this.getName()) //
                    .append(" -> [") //
                    .append( //
                            this.outwardNames().parallel() //
                                    .collect(Collectors.joining(", "))) //
                    .append(']').toString();
        }
    }

    public static ListenablePromise<Throwable> submit(Graph<Promises.UncheckedRunnable> dag) {
        ConcurrentMap<String, SettablePromise<Throwable>> vertext_status = dag.vertexs().parallel().collect( //
                Collectors.toConcurrentMap(
                        Graph.Vertex::getName, //
                        (vertex) -> SettablePromise.create()
                )
        );

        return dag.flip().vertexs().parallel() //
                .map((vertext) -> {
                    return vertext.outwardNames().parallel() //
                            .map((name) -> Promises.decorate(vertext_status.get(name))) //
                            .reduce((left, right) -> {
                                return left.transformAsync((left_exeption) -> {
                                    if (left_exeption != null) {
                                        return left;
                                    }

                                    return right;
                                });
                            }) //
                            .orElse(Promises.immediate(null)) //
                            .transformAsync((throwable) -> {
                                SettablePromise<Throwable> status = vertext_status.get(vertext.getName());
                                if (throwable == null) {
                                    vertext.getPayload().orElse( //
                                            () -> LOGGER.warn("vertext:" + vertext.getName() + " is not set properly") //
                                    ).run();
                                    status.set(null);
                                } else {
                                    status.set(throwable);
                                }
                                return status;
                            }); //
                }) //
                .reduce((left, right) -> {
                    return left.transformAsync((left_exeption) -> {
                        if (left_exeption != null) {
                            return left;
                        }

                        return right;
                    });
                }) //
                .orElse(Promises.immediate(null));
    }


    protected ConcurrentMap<String, Vertex> vertexs;

    public Graph() {
        this.vertexs = new ConcurrentHashMap<>();
    }

    public Vertex newVertex(String name, Payload payload) {
        return this.vertexs.compute(name, (key, old) -> {
            if (old == null) {
                return new Vertex(name, payload);
            } else if (!old.getPayload().isPresent()) {
                return old.bind(payload);
            } else {
                throw new IllegalStateException("vertex:" + name + " already exists");
            }
        });
    }

    public Vertex vertex(String name) {
        return this.vertexs.compute(name, (key, old) -> {
            if (old == null) {
                old = new Vertex(name, null);
            }
            return old;
        });
    }

    public Stream<Vertex> vertexs() {
        return this.vertexs.values().parallelStream();
    }

    public Graph<Payload> flip() {
        Graph<Payload> fliped = new Graph<>();
        this.vertexs().parallel().forEach((vertex) -> {
            ImmutableList.of(
                    Promises.submit(() -> {
                        vertex.outwards().parallel().forEach((outward) -> {
                            fliped.vertex(outward.getName()).link(vertex.getName());
                        });
                    }),
                    Promises.submit(() -> {
                        fliped.newVertex(vertex.getName(), vertex.getPayload().orElse(null));
                        return true;
                    }) //
            ).forEach(
                    (future) -> future.unchecked()
            );
        });

        return fliped;
    }

    public Graph<Payload> copy() {
        Graph<Payload> copyed = new Graph<>();
        this.vertexs().parallel().forEach((vertex) -> {
            Vertex new_vertex = copyed.newVertex(vertex.getName(), vertex.getPayload().orElse(null));
            vertex.outwards().parallel().forEach((outward) -> {
                new_vertex.link(outward.getName());
            });
        });

        return copyed;
    }

    @Override
    public String toString() {
        return this.vertexs().map(Vertex::toString).collect(Collectors.joining("\n"));
    }
}
