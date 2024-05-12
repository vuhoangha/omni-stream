package io.github.vuhoangha.OneToManyStateless;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Arrays;

/**
 * Sử dụng để truyền dữ liệu xử lý msg giữa các tiến trình trong Artemis
 */
@Data
@NoArgsConstructor
public class ArtemisProcessMsg {


    // loại msg. Sẽ ăn theo "Constance.ARTEMIS.PROCESSS_MSG_TYPE"
    private byte type;

    private byte[] data;

    @Override
    public String toString() {
        return "ArtemisProcessMsg{" +
                "type=" + type +
                ", data='" + Arrays.toString(data) + '\'' +
                '}';
    }
}
