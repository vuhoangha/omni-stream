package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.nio.ByteBuffer;

/**
 * Msg dùng để cache trong Artemis
 */
@Data
public class ArtemisCacheMsg {

    private long version;

    private long sequence;

    private final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();

    // thời gian msg này được ghi nhận
    private long rcvTime;

}
