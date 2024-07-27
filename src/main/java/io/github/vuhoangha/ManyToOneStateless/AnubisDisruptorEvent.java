package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.common.Promise;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Dùng làm event của disruptor trong Anubis, nơi sẽ hứng các message của ứng dụng và xử lý
 */
public class AnubisDisruptorEvent {

    public enum TypeEnum {
        SENDING,           // gửi dữ liệu sang Saraswati
        SENT,              // nhận được phản hồi từ Saraswati rồi
        SCAN_TIMEOUT       // quét các req bị timeout
    }

    // event này thuộc loại gì ?
    public TypeEnum type;

    //region SENT
    // nếu có sẵn reqId rồi chứng tỏ dữ liệu đã được gửi đi và giờ cần phản hồi lại cho ứng dụng
    public long reqId;
    //endregion

    //region SENDING
    // dữ liệu ứng dụng gửi
    public Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
    // callback lại cho ứng dụng biết gửi thành công hay không
    public Promise<Boolean> callback;
    // thời gian message hết hạn ở local Anubis
    public long sendingTime;
    //endregion

}
