package io.github.vuhoangha.OneToMany;

import java.util.Arrays;

/**
 * Dùng để xác định 1 yêu cầu lấy miss msg hoặc lấy msg mới nhất
 */
public class CheckMissMsg {

    // ăn theo Constance.FANOUT.CONFIRM
    private byte type;

    private long indexFrom;

    private long indexTo;

    public CheckMissMsg() {
    }


    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public long getIndexFrom() {
        return indexFrom;
    }

    public void setIndexFrom(long indexFrom) {
        this.indexFrom = indexFrom;
    }

    public long getIndexTo() {
        return indexTo;
    }

    public void setIndexTo(long indexTo) {
        this.indexTo = indexTo;
    }

    @Override
    public String toString() {
        return "CheckMissMsg{" +
                "type=" + type +
                ", indexFrom='" + indexFrom + '\'' +
                ", indexTo='" + indexTo + '\'' +
                '}';
    }
}
