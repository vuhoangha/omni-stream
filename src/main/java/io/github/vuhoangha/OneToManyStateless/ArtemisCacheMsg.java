package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;

/**
 * Msg dùng để cache trong Artemis
 */
@Data
public class ArtemisCacheMsg<T extends SelfDescribingMarshallable> {

    private long version;

    private long sequence;

    private T data;

    // thời gian msg này được ghi nhận
    private long rcvTime;

}
