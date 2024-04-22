package io.github.vuhoangha.OneToMany;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.ReferenceOwner;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Sử dụng để truyền dữ liệu xử lý msg giữa các tiến trình trong Sink
 * Nếu là msg data lấy từ PUB/SUB, sẽ bóc byte đầu tiên ra để xác định 'type'
 * Nếu là msg data lấy từ REQ/REP thì type sẽ được gán mặc định = "Constance.SINKIN.PROCESS_MSG_TYPE.NEW_MSG"
 * Nếu là msg check xem có msg nào nằm trong hàng đợi quá lâu thì type = "Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS"
 */
public class SinkProcessMsg {


    // loại msg. Sẽ ăn theo "Constance.SINKIN.PROCESS_MSG_TYPE"
    private byte type;

    // dữ liệu chính của msg
    // có dạng ["version"]["data length"]["data"]["seq"]["index"]
    private final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();


    public SinkProcessMsg() {
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public Bytes<ByteBuffer> getData() {
        return data;
    }

    public void clear() {
        data.clear();
    }

    public void destroy(ReferenceOwner refId) {
        data.release(refId);
    }

    @Override
    public String toString(){
        return "SinkProcessMsg{" +
                "type=" + type +
                ", data='" + Arrays.toString(data.toByteArray()) + '\'' +
                '}';
    }
}
