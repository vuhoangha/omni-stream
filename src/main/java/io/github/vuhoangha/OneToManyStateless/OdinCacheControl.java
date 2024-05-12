package io.github.vuhoangha.OneToManyStateless;

import lombok.Getter;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class OdinCacheControl {

    // chứa mapping của time và sequence
    private NavigableMap<Long, Long> timeSeqMap = new TreeMap<>();
    // chứa mapping của sequence và object
    private Map<Long, OdinCacheEvent> seqObjMap = new ConcurrentHashMap<>();
    // thông tin event gần nhất
    @Getter
    private byte[] latestEventData = new byte[0];


    // tìm cách phần tử nằm trong một khoảng sequence. Nếu phần tử gần hết hạn thì bỏ qua
    public void findBetweenSequence(long start, long end, Bytes<ByteBuffer> byteOuts) {
        for (long i = start; i <= end; i++) {
            OdinCacheEvent event = seqObjMap.getOrDefault(i, null);
            if (event != null) {
                byteOuts.write(event.getData());
            }
        }
    }


    // thêm 1 phần tử vào cache. Chỉ 1 luồng thao tác 1 lúc
    public void add(OdinCacheEvent event) {
        timeSeqMap.put(event.getTime(), event.getSeq());
        seqObjMap.put(event.getSeq(), event);
        latestEventData = event.getData();
    }


    // xóa các phần tử quá cũ và trả về các phần tử này. Chỉ 1 luồng thao tác 1 lúc
    public void removeExpiry(long timeToLive, Consumer<OdinCacheEvent> consumer) {
        long smallestTime = System.currentTimeMillis() - timeToLive;
        NavigableMap<Long, Long> headMap = timeSeqMap.headMap(smallestTime, false);
        Iterator<Map.Entry<Long, Long>> it = headMap.entrySet().iterator();
        while (it.hasNext()) {
            long seq = it.next().getValue();
            OdinCacheEvent item = seqObjMap.getOrDefault(seq, null);
            if (item != null) {
                consumer.accept(item);
                seqObjMap.remove(seq);
            }
            it.remove();
        }
    }

}
