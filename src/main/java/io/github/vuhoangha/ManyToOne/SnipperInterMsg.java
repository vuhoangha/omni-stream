package io.github.vuhoangha.ManyToOne;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

public class SnipperInterMsg<T extends SelfDescribingMarshallable> {

    private long id;

    private T data;

    private long expiry;

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

    public long getExpiry() {
        return expiry;
    }

    public void setExpiry(long expiry) {
        this.expiry = expiry;
    }

    @Override
    public String toString() {
        return "SnipperInterMsg{" +
                "id=" + id +
                ", data='" + data.toString() + '\'' +
                ", expiry='" + expiry + '\'' +
                '}';
    }
}
