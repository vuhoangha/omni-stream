package io.github.vuhoangha.Common;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

public interface SinkinHandler {

    // localIndex: index của queue ở local, ko phải của source nhé
    // sequence: thứ tự item trong queue
    // data: dữ liệu của item
    void apply(long localIndex, long sequence, Bytes<ByteBuffer> data);

}
