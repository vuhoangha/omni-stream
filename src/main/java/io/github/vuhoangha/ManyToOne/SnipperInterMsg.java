package io.github.vuhoangha.ManyToOne;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class SnipperInterMsg<T extends SelfDescribingMarshallable> {

    private long id;

    private T data;

    public SnipperInterMsg() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
