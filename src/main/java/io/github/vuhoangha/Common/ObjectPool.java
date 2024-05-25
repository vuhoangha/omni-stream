package io.github.vuhoangha.Common;

import java.util.ArrayDeque;
import java.util.function.Supplier;

/**
 * ObjectPool sử dụng trong môi trường đơn luồng
 */
public class ObjectPool<T> {

    private final ArrayDeque<T> deque = new ArrayDeque<>();
    private final int maxSize;
    private final Supplier<T> factory;

    public ObjectPool(int maxSize, Supplier<T> factory) {
        this.maxSize = maxSize;
        this.factory = factory;
    }

    public void push(T element) {
        if (deque.size() < maxSize)
            deque.push(element);
    }

    public T pop() {
        if (deque.isEmpty())
            return factory.get();
        return deque.pop();
    }

    public void clear() {
        while (!deque.isEmpty())
            deque.pop();
    }

}
