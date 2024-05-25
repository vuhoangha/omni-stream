package io.github.vuhoangha.Common;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

public class ConcurrentObjectPool<T> {

    private final ConcurrentLinkedQueue<T> pool;
    private final Supplier<T> factory;
    private final int maxSize;

    public ConcurrentObjectPool(int maxSize, Supplier<T> factory) {
        this.maxSize = maxSize;
        this.factory = factory;
        this.pool = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < maxSize; i++) {
            pool.add(factory.get());
        }
    }

    public T pop() {
        T object = pool.poll();
        return (object != null) ? object : factory.get();
    }

    public void push(T object) {
        if (pool.size() < maxSize) {
            pool.offer(object);
        }
    }

}
