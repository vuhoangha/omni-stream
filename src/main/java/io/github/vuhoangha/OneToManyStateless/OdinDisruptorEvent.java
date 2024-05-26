package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Event sử dụng trong Lmax Disruptor
 */
@Data
public class OdinDisruptorEvent {

    private final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();

    // dữ liệu nhị phân dùng gửi cho Artemis
    // [version][seq][data] --> binary
    private byte[] binary;

}
