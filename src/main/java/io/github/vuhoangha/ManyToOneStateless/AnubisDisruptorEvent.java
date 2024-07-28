package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.common.Promise;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Dùng làm event của disruptor trong Anubis, nơi sẽ hứng các message của ứng dụng và xử lý
 */
public class AnubisDisruptorEvent {

    // dữ liệu ứng dụng gửi
    public Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

    // callback lại cho ứng dụng biết gửi thành công hay không
    public Promise<Boolean> callback;

    // thời gian message hết hạn ở local Anubis
    public long sendingTime;

}
