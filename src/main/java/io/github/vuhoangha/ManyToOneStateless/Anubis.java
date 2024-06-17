package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.common.Promise;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Anubis {

    private final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    private final AnubisCfg _cfg;
    private Disruptor<Bytes<ByteBuffer>> _disruptor_send_msg;
    private RingBuffer<Bytes<ByteBuffer>> _ring_buffer_send_msg;
    private final ZContext _zmq_context;
    private AnubisProcessor _processor;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());

    // map id của item với thời gian tối đa nó chờ bên Saraswati xác nhận
    private final ConcurrentNavigableMap<Long, Long> _map_item_with_time = new ConcurrentSkipListMap<>();
    // map id của item với callback để call lại khi cần
    private final ConcurrentHashMap<Long, Promise<Boolean>> _map_item_with_callback = new ConcurrentHashMap<>();
    // quản lý id của các request. ID sẽ increment sau mỗi request
    private final AtomicLong _sequence_id = new AtomicLong(System.currentTimeMillis());
    // độ trễ thời gian giữa Anubis và Saraswati = anubis_time - saraswati_time
    private final AtomicLong _time_latency = new AtomicLong(0);


    public Anubis(AnubisCfg cfg) {
        // validate
        Utils.checkNull(cfg.getSaraswatiIP(), "Require SaraswatiIP");

        _cfg = cfg;
        _zmq_context = new ZContext();
        _status.set(RUNNING);

        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Anubis Disruptor",
                        false,
                        false,
                        Constance.CPU_TYPE.NONE,
                        _cfg.getEnableDisruptorBindingCore(),
                        _cfg.getDisruptorCpu(),
                        this::_initDisruptor));

        // lắng nghe time từ server
        new Thread(this::_listenTimeServer).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    /**
     * Gom nhiều message được gửi lại và chuyển cho 1 thread gửi đi
     * đồng thời nhận cả phản hồi từ Saraswati
     */
    private void _initDisruptor() {
        log.info("Anubis run Disruptor on logical processor {}", Affinity.getCpu());

        _disruptor_send_msg = new Disruptor<>(
                Bytes::elasticByteBuffer,
                _cfg.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                _cfg.getWaitStrategy());
        _disruptor_send_msg.start();
        _ring_buffer_send_msg = _disruptor_send_msg.getRingBuffer();
        _processor = new AnubisProcessor(
                _ring_buffer_send_msg,
                _zmq_context,
                _cfg.getUrl(),
                _cfg.getDisruptorWaitStrategy(),
                _map_item_with_time,
                _map_item_with_callback);
        new Thread(_processor).start();
    }


    public void sendAsync(WriteBytesMarshallable data, Promise<Boolean> cb) {

        long reqId = _sequence_id.incrementAndGet();

        try {

            // quản lý thời gian timeout
            _map_item_with_time.put(reqId, System.currentTimeMillis() + _cfg.getTimeout());

            // quản lý callback trả về
            _map_item_with_callback.put(reqId, cb);

            // gửi sang luồng chính để gửi cho core
            _ring_buffer_send_msg.publishEvent(
                    (newEvent, sequence, __id, __data, __expiry) -> {
                        // dữ liệu này sẽ dùng gửi thẳng sang Saraswati
                        // ["time_to_live"]["req_id"]["data"]
                        newEvent.clear();
                        newEvent.writeLong(__expiry);
                        newEvent.writeLong(__id);
                        __data.writeMarshallable(newEvent);
                    },
                    reqId, data, getExpiry());

        } catch (Exception ex) {
            log.error("Anubis send error, data {}", data.toString(), ex);

            _map_item_with_time.remove(reqId);
            _map_item_with_callback.remove(reqId);
            cb.completeWithException(ex);
        }
    }


    public boolean send(WriteBytesMarshallable data, Promise<Boolean> cb, long timeInterval) {
        try {
            sendAsync(data, cb);
            return cb.get(timeInterval);
        } catch (Exception ex) {
            return false;
        }
    }


    public boolean send(WriteBytesMarshallable data, long timeInterval) {
        Promise<Boolean> cb = new Promise<>();
        return send(data, cb, timeInterval);
    }


    public boolean send(WriteBytesMarshallable data) {
        return send(data, 1_000_000L);
    }


    private long getExpiry() {
        return System.currentTimeMillis() + _cfg.getTtl() - _time_latency.get();
    }


    // cơ chế để căn chỉnh thời gian bên gửi và bên nhận ít chênh lệch nhất có thể, phục vụ cho việc set expiry cho msg gửi đi
    private void _listenTimeServer() {
        // khởi tạo socket
        ZMQ.Socket socket = _zmq_context.createSocket(SocketType.DEALER);
        socket.setHeartbeatIvl(10000);
        socket.setHeartbeatTtl(15000);
        socket.setHeartbeatTimeout(15000);
        socket.setReconnectIVL(10000);
        socket.setReconnectIVLMax(10000);
        socket.connect(_cfg.getTimeServerUrl());

        byte[] reply;
        long clientTime, systemTime;
        Bytes<ByteBuffer> bytesResponse = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesRequest = Bytes.elasticByteBuffer();
        long nextTimeRequest = 0;

        while (_status.get() == RUNNING) {
            try {
                long now = System.currentTimeMillis();

                if (nextTimeRequest < now) {
                    nextTimeRequest = now + _cfg.getSyncTimeServerInterval();
                    bytesRequest.writeLong(now);
                    socket.send(bytesRequest.toByteArray(), 0);
                    bytesRequest.clear();
                } else {
                    reply = socket.recv(ZMQ.NOBLOCK);
                    if (reply != null) {
                        bytesResponse.write(reply);
                        clientTime = bytesResponse.readLong();
                        systemTime = bytesResponse.readLong();
                        long diff = now - clientTime;
                        if (diff <= 1000)   // các msg phản hồi dưới 1s mới tính
                            _time_latency.set(((clientTime + now) / 2) - systemTime);
                        bytesResponse.clear();
                    }
                }

                // tuy rằng sleep tạm thời cho CPU nghỉ ngơi nhưng nó cũng gây ảnh hưởng tới latency đo được
                // nghỉ càng nhiều sai số càng lớn
                LockSupport.parkNanos(50_000_000L);
            } catch (Exception ex) {
                bytesRequest.clear();
                bytesResponse.clear();
                log.error("Anubis ListenTimeServer error", ex);
            }
        }

        bytesResponse.releaseLast();
        bytesRequest.releaseLast();
        socket.close();
    }


    private void _onShutdown() {
        log.info("Anubis closing...");

        _status.set(STOPPED);

        _map_item_with_time.clear();
        _map_item_with_callback.clear();

        // disruptor
        _disruptor_send_msg.shutdown();
        _processor.halt();                      // stop processor
        LockSupport.parkNanos(500_000_000);   // tạm ngừng để xử lý nốt msg trong ring buffer

        // zmq
        _zmq_context.destroy();

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(1_500_000_000);

        log.info("Anubis SHUTDOWN !");
    }

}
