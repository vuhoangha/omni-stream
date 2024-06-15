package io.github.vuhoangha.OneToMany;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

import java.util.Arrays;

/**
 * Sử dụng để truyền dữ liệu xử lý msg giữa các tiến trình trong Sink
 * Nếu là msg data lấy từ PUB/SUB, sẽ bóc byte đầu tiên ra để xác định 'type'
 * Nếu là msg data lấy từ REQ/REP thì type sẽ được gán mặc định = "Constance.SINKIN.PROCESS_MSG_TYPE.NEW_MSG"
 * Nếu là msg check xem có msg nào nằm trong hàng đợi quá lâu thì type = "Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS"
 */
@Setter
@Getter
@Accessors(chain = true)
public class SinkProcessMsg {


    // loại msg. Sẽ ăn theo "Constance.SINKIN.PROCESS_MSG_TYPE"
    private byte type;

    // dữ liệu chính của msg, từ cả sub msg và confirm msg nên cấu trúc sẽ khác nhau chút
    private byte[] data = null;

    @Override
    public String toString() {
        return "SinkProcessMsg{" +
                "type=" + type +
                ", data='" + Arrays.toString(data) + '\'' +
                '}';
    }
}
