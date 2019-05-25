package com.raysurf.test.jinx.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Test1 {
    public static void main(String[] args) {
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
            int i = 1/1;
            return 100;
        });
       // System.out.println(future.join());
        try {
            future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
