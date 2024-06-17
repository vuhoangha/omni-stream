package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.ReflectionUtils;
import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.common.Promise;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;


/*
 * vì Anubis dùng Lmax disruptor để lắng nghe các msg ghi từ nhiều thread và gửi sang Saraswati qua ZMQ
 * ZMQ tối ưu và hoạt động chuẩn nhất khi mỗi socket chạy một thread riêng nên ở đây tạo riêng 1 processor là "AnubisProcessor"
 *      để chạy một thread độc lập, đọc dữ liệu từ Lmax và gửi sang Saraswati
 */
@Slf4j
public class AnubisProcessor implements Runnable {

    private final RingBuffer<Bytes<ByteBuffer>> _ring_buffer;
    private final Sequencer _sequencer;
    private final Sequence _sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);
    private volatile boolean _running = true;
    private final ZContext _zContext;
    private final String _socket_url;
    // map id của item với thời gian tối đa nó chờ bên Saraswati xác nhận
    private final ConcurrentNavigableMap<Long, Long> _map_item_with_time;
    // map id của item với callback để call lại khi cần
    private final ConcurrentHashMap<Long, Promise<Boolean>> _map_item_with_callback;
    // bao lâu quét check timeout 1 lần
    private final long _time_out_interval_ms = 1000;
    // chiến lược nghỉ ngơi giữa các vòng lặp
    private final OmniWaitStrategy _wait_strategy;


    public AnubisProcessor(
            RingBuffer<Bytes<ByteBuffer>> ringBuffer,
            ZContext context,
            String socketUrl,
            OmniWaitStrategy waitStrategy,
            ConcurrentNavigableMap<Long, Long> mapItemWithTime,
            ConcurrentHashMap<Long, Promise<Boolean>> mapItemWithCallback) {
        this._ring_buffer = ringBuffer;
        this._sequencer = ReflectionUtils.extractSequencer(ringBuffer);
        this._zContext = context;
        this._socket_url = socketUrl;
        this._map_item_with_time = mapItemWithTime;
        this._map_item_with_callback = mapItemWithCallback;
        this._wait_strategy = waitStrategy;
    }


    public void halt() {
        _running = false;
    }


    @Override
    public void run() {
        log.info("Anubis run Anubis Processor on logical processor {}", Affinity.getCpu());

        // khởi tạo socket
        ZMQ.Socket socket = _zContext.createSocket(SocketType.DEALER);
        socket.setRcvHWM(1000000);
        socket.setHeartbeatIvl(30000);
        socket.setHeartbeatTtl(45000);
        socket.setHeartbeatTimeout(45000);
        socket.setReconnectIVL(10000);
        socket.setReconnectIVLMax(10000);
        socket.connect(_socket_url);

        long nextSequence = _sequence.get() + 1L;
        long availableSequence;
        byte[] reply;
        long nextTimeCheckTimeout = System.currentTimeMillis() + _time_out_interval_ms;     // lần check timeout tiếp theo
        long reqID;

        Runnable waiter = OmniWaitStrategy.getWaiter(_wait_strategy);

        // luồng chính
        while (_running) {
            try {
                // xem có msg nào cần gửi đi ko
                availableSequence = _sequencer.getHighestPublishedSequence(nextSequence, _ring_buffer.getCursor());     // lấy sequence được publish cuối cùng trong ring_buffer
                if (nextSequence <= availableSequence) {
                    while (nextSequence <= availableSequence) {
                        Bytes<ByteBuffer> newEvent = _ring_buffer.get(nextSequence);
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
                        reqID = Utils.bytesToLong(reply);
                        _map_item_with_time.remove(reqID);
                        Promise<Boolean> cb = _map_item_with_callback.remove(reqID);
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
                            Promise<Boolean> cb = _map_item_with_callback.remove(firstEntry.getKey());
                            cb.complete(false);
                            log.warn("Anubis send msg timeout");
                        } else {
                            // dừng tìm kiếm vì key sắp xếp tăng dần và key-value tăng tỉ lệ thuận
                            break;
                        }
                    }
                    nextTimeCheckTimeout += _time_out_interval_ms;
                }

                // cho CPU nghỉ ngơi 1 chút
                waiter.run();
            } catch (Exception ex) {
                log.error("AnubisProcessor run error", ex);
            }
        }

        socket.close();
    }


    // dữ liệu gửi đi ["time_to_live"]["req_id"]["data"]
    // nó đã được tổng hợp từ khi user gửi vào
    private void _send(ZMQ.Socket socket, Bytes<ByteBuffer> msg) {
        try {
            socket.send(msg.toByteArray(), ZMQ.NOBLOCK);
        } catch (Exception ex) {
            log.error("AnubisProcessor send error, msg {}", msg.toString(), ex);
        }
    }


}
