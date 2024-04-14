package io.github.vuhoangha.ManyToOne;

import com.lmax.disruptor.*;
import io.github.vuhoangha.Common.ReflectionUtils;
import io.github.vuhoangha.Common.Utils;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.LockSupport;

public class SnipperProcessor implements Runnable {

    private final RingBuffer<SnipperInterMsg> _ring_buffer;
    private final Sequencer _sequencer;
    private final Sequence _sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private volatile boolean _running = true;
    private final ZContext _zContext;
    private final String _socket_url;
    // dùng để serialize data gửi sang Collector
    private final Bytes<ByteBuffer> _bytes_req = Bytes.elasticByteBuffer();
    private final Wire _wire_req = WireType.BINARY.apply(_bytes_req);
    // map id của item với thời gian tối đa nó chờ bên Collector xác nhận
    private final ConcurrentNavigableMap<Long, Long> _map_item_with_time;
    // map id của item với callback để call lại khi cần
    private final ConcurrentHashMap<Long, CompletableFuture<Boolean>> _map_item_with_callback;
    // bao lâu quét check timeout 1 lần
    private final long _time_out_interval_ms = 1000;


    public SnipperProcessor(
            RingBuffer<SnipperInterMsg> ringBuffer,
            ZContext context,
            String socketUrl,
            ConcurrentNavigableMap<Long, Long> mapItemWithTime,
            ConcurrentHashMap<Long, CompletableFuture<Boolean>> mapItemWithCallback) {
        this._ring_buffer = ringBuffer;
        this._sequencer = ReflectionUtils.extractSequencer(ringBuffer);
        this._zContext = context;
        this._socket_url = socketUrl;
        this._map_item_with_time = mapItemWithTime;
        this._map_item_with_callback = mapItemWithCallback;
    }


    public void halt() {
        _running = false;

        _bytes_req.releaseLast();
    }


    @Override
    public void run() {
        // khởi tạo socket
        ZMQ.Socket socket = _zContext.createSocket(SocketType.DEALER);
        socket.setRcvHWM(1000000);
        socket.setHeartbeatIvl(10000);
        socket.setHeartbeatTtl(15000);
        socket.setHeartbeatTimeout(15000);
        socket.setReconnectIVL(10000);
        socket.setReconnectIVLMax(10000);
        socket.connect(_socket_url);

        long nextSequence = _sequence.get() + 1L;
        long availableSequence;
        byte[] reply;
        SnipperInterMsg newEvent;
        long nextTimeCheckTimeout = System.currentTimeMillis() + _time_out_interval_ms;     // lần check timeout tiếp theo
        long id;

        // luồng chính
        while (_running) {

            // xem có msg nào cần gửi đi ko
            availableSequence = _sequencer.getHighestPublishedSequence(nextSequence, _ring_buffer.getCursor());     // lấy sequence được publish cuối cùng trong ring_buffer
            if (nextSequence <= availableSequence) {
                while (nextSequence <= availableSequence) {
                    newEvent = _ring_buffer.get(nextSequence);
                    _send(socket, newEvent);
                    nextSequence++;
                }
                _sequence.set(availableSequence);    // di chuyển tới sequence cuối cùng ghi nhận
            }

            // check xem có nhận đc msg mới không?
            while (true) {
                reply = socket.recv(ZMQ.NOBLOCK);
                if (reply != null) {
                    // xóa khỏi cache --> callback về
                    id = Utils.bytesToLong(reply);
                    _map_item_with_time.remove(id);
                    CompletableFuture<Boolean> cb = _map_item_with_callback.remove(id);
                    if (cb != null)
                        cb.complete(true);
                } else {
                    break;
                }
            }

            // check xem có msg nào bị timeout ko
            long nowMS = System.currentTimeMillis();
            if (nextTimeCheckTimeout < nowMS) {
                while (!_map_item_with_time.isEmpty()) {
                    Map.Entry<Long, Long> firstEntry = _map_item_with_time.firstEntry();
                    if (firstEntry.getValue() < nowMS) {
                        // bị timeout --> xóa khỏi cache --> callback về
                        _map_item_with_time.remove(firstEntry.getKey());
                        CompletableFuture<Boolean> cb = _map_item_with_callback.remove(firstEntry.getKey());
                        cb.complete(false);
                    } else {
                        // dừng tìm kiếm vì key sắp xếp tăng dần và key-value tăng tỉ lệ thuận
                        break;
                    }
                }
                nextTimeCheckTimeout += _time_out_interval_ms;
            }

            // cho CPU nghỉ ngơi 1 chút
            LockSupport.parkNanos(1);
        }

        // đóng socket
        socket.close();
    }


    // dữ liệu gửi đi ["id"]["data"]
    private void _send(ZMQ.Socket socket, SnipperInterMsg msg) {
        _bytes_req.writeLong(msg.getId());
        msg.getData().writeMarshallable(_wire_req);

        socket.send(_bytes_req.toByteArray(), 0);

        _bytes_req.clear();
        _wire_req.clear();
    }


}
