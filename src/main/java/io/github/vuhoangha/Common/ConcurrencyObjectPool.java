package io.github.vuhoangha.Common;

import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * ObjectPool dùng trong môi trường đa luồng
 */
public class ConcurrencyObjectPool<T> {

    private final ConcurrentLinkedDeque<T> deque = new ConcurrentLinkedDeque<>();
    private final int maxSize;
    private final Class<T> dataType;

    public ConcurrencyObjectPool(int maxSize, Class<T> dataType) {
        this.maxSize = maxSize;
        this.dataType = dataType;
    }

    public void push(T element) {
        if (deque.size() < maxSize)
            deque.push(element);
    }

    public T pop() {
        try {
            if (deque.isEmpty())
                return this.dataType.newInstance();
            return deque.pop();
        } catch (Exception e) {
            return null;
        }
    }

    public void clear() {
        deque.clear();
    }

}
