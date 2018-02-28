package com.sf.misc.async;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class TestExecutorServices {

    @Test
    public void test() {
        Graph<ExecutorServices.Lambda> graph = new Graph<>();
        /*
                      1       5
                     / | \   /
                    2 3  4  6
                          \/
                          7
         */
        ConcurrentHashMap<Integer, Boolean> flags = new ConcurrentHashMap<>();
        graph.newVertex("vertext-1", () -> flags.put(1, true)) //
                .link("vertext-2").link("vertext-3").link("vertext-4") //
                .graph().newVertex("vertext-2", () -> {
            Assert.assertTrue(flags.get(1));
            flags.put(2, true);
        }) //
                .graph().newVertex("vertext-3", () -> {
            Assert.assertTrue(flags.get(1));
            flags.put(3, true);
        }) //
                .graph().newVertex("vertext-4", () -> {
            Assert.assertTrue(flags.get(1));
            flags.put(4, true);
        }).link("vertext-7") //
                .graph().newVertex("vertext-7", () -> {
            Assert.assertTrue(flags.get(4));
            Assert.assertTrue(flags.get(6));
        }) //
                .graph().newVertex("vertext-5", () -> flags.put(5, true)) //
                .link("vertext-6") //
                .graph().newVertex("vertext-6", () -> {
            Assert.assertTrue(flags.get(5));
            flags.put(6, true);
        }) //
                .link("vertext-7");
        System.out.println(graph);
        try {
            ExecutorServices.submit(graph).get();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }

    }
}
