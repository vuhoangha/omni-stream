package io.github.vuhoangha.Common;

import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

public interface SinkinHandler {

    /**
     * @param localIndex   index của queue ở local, ko phải của source nhé
     * @param currentSeq   seq trong queue hiện tại
     * @param endSyncedSeq seq mà kết thúc việc đồng bộ dữ liệu ban đầu với Fanout. Msg ở seq này và các msg trước đó được coi là các msg cũ. Các msg sau này sẽ là msg mới được realtime
     * @param data         dữ liệu của item
     */
    void apply(long localIndex, long currentSeq, long endSyncedSeq, Bytes<ByteBuffer> data);

}
