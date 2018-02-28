package com.sf.misc.async;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Graph<Payload> {

    protected class Vertex {

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

        @Override
        public String toString() {
            return new StringBuilder().append(this.getName()) //
                    .append(" -> [") //
                    .append( //
                            this.outwards().parallel() //
                                    .map(Vertex::getName) //
                                    .collect(Collectors.joining(", "))) //
                    .append(']').toString();
        }
    }

    protected class PlaceHolderVertex extends Vertex {
        public PlaceHolderVertex(String name, Payload payload) {
            super(name, payload);
        }

        public Vertex bind(Payload payload) {
            Vertex vertex = new Vertex(this.name, payload);
            this.outwards().parallel().forEach((outward) -> {
                vertex.link(outward.getName());
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

    @Override
    public String toString() {
        return  this.vertexs().map(Vertex::toString).collect(Collectors.joining("\n"));
    }
}
