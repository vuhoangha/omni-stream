package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

/**
 * Event sử dụng trong Lmax Disruptor
 */
@Data
public class OdinDisruptorEvent<T extends WriteBytesMarshallable> {

    private T data;

    // dữ liệu nhị phân dùng gửi cho Artemis
    // [version][seq][data] --> binary
    private byte[] binary;

}
