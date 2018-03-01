package com.sf.misc.async;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Graph<Payload> {

    public class Vertex {

        protected Optional<Payload> payload;

        protected String name;

        protected Set<String> outwards;

        public Vertex(String name, Payload payload) {
            this.payload = Optional.ofNullable(payload);
            this.name = name;
            this.outwards = new ConcurrentSkipListSet<>();
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

        public Stream<String> outwardNames(){
            return this.outwards.parallelStream();
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

    protected class PlaceHolderVertex extends Vertex {
        public PlaceHolderVertex(String name, Payload payload) {
            super(name, payload);
        }

        private Vertex bind(Payload payload) {
            Vertex vertex = new Vertex(this.name, payload);

            // do not use outwards()
            // as it indirectly request read lock of graph.vertexs,
            // this will lead to deadlock as bind is maily call
            // as a lazy/delay bind for associated named vertex
            // which means in a modify state of graph.vertexs.
            // a write lock is potentially hold by differenct thread
            this.outwardNames().parallel().forEach((outward) -> {
                vertex.link(outward);
            });

            return vertex;
        }
    }

    protected ConcurrentMap<String, Vertex> vertexs;

    public Graph() {
        this.vertexs = new ConcurrentHashMap<>();
    }

    public Vertex newVertex(String name, Payload payload) {
        return this.vertexs.compute(name, (key, old) -> {
            if (old == null) {
                return new Vertex(name, payload);
            } else if (old.getClass().isAssignableFrom(PlaceHolderVertex.class)) {
                return ((PlaceHolderVertex) old).bind(payload);
            } else {
                throw new IllegalStateException("vertex:" + name + " already exists");
            }
        });
    }

    public Vertex vertex(String name) {
        return this.vertexs.compute(name, (key, old) -> {
            if (old == null) {
                old = new PlaceHolderVertex(name, null);
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
            Arrays.asList(
                    ExecutorServices.executor().submit(() -> {
                        vertex.outwards().parallel().forEach((outward) -> {
                            fliped.vertex(outward.getName()).link(vertex.getName());
                        });
                    }),
                    ExecutorServices.executor().submit(() -> {
                        fliped.newVertex(vertex.getName(), vertex.getPayload().orElse(null));
                        return true;
                    }) //
            ).forEach(
                    (future) -> ((ExecutorServices.Lambda) (() -> future.get())).run()
            );
        });

        return fliped;
    }

    @Override
    public String toString() {
        return this.vertexs().map(Vertex::toString).collect(Collectors.joining("\n"));
    }
}
