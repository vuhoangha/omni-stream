package io.github.vuhoangha.OneToManyStateless;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

public interface ArtemisHandler {

    void apply(long sequence, Bytes<ByteBuffer> data);

}
