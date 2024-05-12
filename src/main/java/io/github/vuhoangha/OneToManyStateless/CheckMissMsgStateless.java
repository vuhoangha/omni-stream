package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Dùng để xác định 1 yêu cầu lấy miss msg hoặc lấy msg mới nhất
 */
@Data
@NoArgsConstructor
public class CheckMissMsgStateless {

    // ăn theo Constance.FANOUT.CONFIRM
    private byte type;

    private long seqFrom;

    private long seqTo;

    @Override
    public String toString() {
        return "CheckMissMsg{" +
                "type=" + type +
                ", seqFrom='" + seqFrom + '\'' +
                ", seqTo='" + seqTo + '\'' +
                '}';
    }
}
