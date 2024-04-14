package io.github.vuhoangha.OneToMany;

import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.ReferenceOwner;

import java.nio.ByteBuffer;

/**
 * Message data dùng để gửi nhận qua network
 */
public class TranspotMsg {

    // dữ liệu của queue item này
    private final Bytes<ByteBuffer> allData = Bytes.elasticByteBuffer();

    // phiên bản của msg
    private byte version;

    // dữ liệu chính của queue item
    private final Bytes<ByteBuffer> data = Bytes.elasticByteBuffer();

    // số thứ tự của message trong queue
    private long seq;

    // index trong native queue ở src
    private long srcIndex;

    // thời gian msg này được ghi nhận
    private long rcvTime;

    public byte getVersion() {
        return version;
    }

    public void setVersion(byte version) {
        this.version = version;
    }

    public Bytes<ByteBuffer> getAllData() {
        return allData;
    }

    public Bytes<ByteBuffer> getData() {
        return data;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public long getSrcIndex() {
        return srcIndex;
    }

    public void setSrcIndex(long srcIndex) {
        this.srcIndex = srcIndex;
    }

    public long getRcvTime() {
        return rcvTime;
    }

    public void setRcvTime(long rcvTime) {
        this.rcvTime = rcvTime;
    }


    public TranspotMsg() {
    }

    /**
     * dùng để lấy dữ liệu kiểu byte[] để insert vào queue từ các field
     * hạn chế dùng vì nó giảm hiệu suất
     * cấu trúc: ["version 1"]["độ dài data 1"]["data 1"]["seq in queue 1"]["source native index 1"]
     */
    public Bytes<ByteBuffer> toBytes() {
        if (allData.isEmpty()) {
            // nếu ko có dữ liệu sẵn thì mới tổng hợp
            allData.clear();
            allData.writeByte(version);
            allData.writeInt((int) data.writePosition());
            allData.write(data);
            allData.writeLong(seq);
            allData.writeLong(srcIndex);
        }
        return allData;
    }

    public void clear() {
        data.clear();
        allData.clear();
    }

    public void destroy(ReferenceOwner id) {
        data.release(id);
        allData.release(id);
    }

    @Override
    public String toString() {
        return "TranspotMsg{" +
                "version=" + version +
                ", seq=" + seq +
                ", srcIndex=" + srcIndex +
                ", rcvTime=" + rcvTime +
                ", data=" + data +
                ", allData=" + allData +
                '}';
    }
}
