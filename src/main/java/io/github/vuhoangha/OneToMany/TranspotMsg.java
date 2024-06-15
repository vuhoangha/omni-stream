package io.github.vuhoangha.OneToMany;

import lombok.Getter;
import lombok.Setter;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

/**
 * Message data dùng để gửi nhận qua network
 */
@Setter
@Getter
public class TranspotMsg {

    // dữ liệu dùng để ghi thẳng vào queue hoặc đọc từ queue. Cấu trúc ["source native index 1"]["seq in queue 1"]["data 1"]
    private final Bytes<ByteBuffer> queueData = Bytes.elasticByteBuffer();

    // dữ liệu chính của queue item
    private final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();

    // số thứ tự của message trong queue
    private long seq;

    // index trong native queue ở src
    private long srcIndex;

    // thời gian msg này được ghi nhận
    private long rcvTime;


    public TranspotMsg() {
    }


    public void clear() {
        data.clear();
        queueData.clear();
    }

    public void destroy() {
        data.releaseLast();
        queueData.releaseLast();
    }

    @Override
    public String toString() {
        return "TranspotMsg{" +
                "seq=" + seq +
                ", srcIndex=" + srcIndex +
                ", rcvTime=" + rcvTime +
                ", data=" + data +
                ", queueData=" + queueData +
                '}';
    }
}
