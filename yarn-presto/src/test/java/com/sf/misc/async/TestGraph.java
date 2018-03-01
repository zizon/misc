package com.sf.misc.async;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestGraph {

    Graph<String> graph;

    @Before
    public void initialize() {
        graph = new Graph<>();
    }

    @Test
    public void testVertex() {
        int nodes = 10;
        for (int i = 0; i < nodes; i++) {
            Assert.assertNotNull("vertext null", graph.newVertex("vertex:" + i, "payload-" + i));
        }
        Assert.assertEquals(nodes, graph.vertexs().count());

        String vertext_name = "vertext-1";
        Graph.Vertex vertex = graph.vertex(vertext_name);
        Assert.assertNotNull("place holder vertext null", vertex);
        Assert.assertTrue(vertex instanceof Graph.PlaceHolderVertex);
        Assert.assertEquals(nodes + 1, graph.vertexs().count());

        Graph.Vertex link_vertext = graph.vertex("link");
        Assert.assertEquals(link_vertext, link_vertext.link(vertext_name));

        vertex.link(link_vertext.getName());
        graph.newVertex(vertext_name,null);
        Assert.assertNotEquals(vertex,graph.vertex(vertext_name));
        Assert.assertEquals(1,graph.vertex(vertext_name).outwards().count());

        Assert.assertEquals(graph.vertexs().count(),graph.flip().vertexs().count());
    }
}
