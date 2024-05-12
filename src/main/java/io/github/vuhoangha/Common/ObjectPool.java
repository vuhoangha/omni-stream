package io.github.vuhoangha.Common;

import java.util.ArrayDeque;

/**
 * ObjectPool sử dụng trong môi trường đơn luồng
 */
public class ObjectPool<T> {

    private final ArrayDeque<T> deque = new ArrayDeque<>();
    private final int maxSize;
    private final Class<T> dataType;

    public ObjectPool(int maxSize, Class<T> dataType) {
        this.maxSize = maxSize;
        this.dataType = dataType;
    }

    public void push(T element) {
        if (deque.size() < maxSize)
            deque.push(element);
    }

    public T pop() throws Exception {
        if (deque.isEmpty())
            return this.dataType.newInstance();
        return deque.pop();
    }

    public void clear() {
        while (!deque.isEmpty())
            deque.pop();
    }

}
