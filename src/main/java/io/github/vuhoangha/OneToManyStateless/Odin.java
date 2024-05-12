package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Odin<T extends SelfDescribingMarshallable> {

    // quản lý trạng thái
    private final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    // số thứ tự item và version
    private long _seq = 0;
    private final long _version = System.nanoTime();

    private final OdinCfg _cfg;
    Bytes<ByteBuffer> _byte_temp_disruptor = Bytes.elasticByteBuffer();
    Wire _wire_temp_disruptor = WireType.BINARY.apply(_byte_temp_disruptor);
    Bytes<ByteBuffer> _byte_disruptor = Bytes.elasticByteBuffer();
    private Disruptor<OdinDisruptorEvent> _disruptor;
    private RingBuffer<OdinDisruptorEvent> _ring_buffer;
    private final ZContext _zmq_context;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());
    private final ConcurrencyObjectPool<OdinCacheEvent> _object_pool;
    private final OdinCacheControl _odin_cache_control = new OdinCacheControl();
    ScheduledExecutorService _remove_expiry_cache_executor = Executors.newScheduledThreadPool(1);
    private final Class<T> _dataType;


    public Odin(OdinCfg cfg, Class<T> dataType) {
        Utils.checkNull(dataType, "Require dataType");

        _cfg = cfg;
        _status.set(RUNNING);
        _dataType = dataType;
        _zmq_context = new ZContext();
        _object_pool = new ConcurrencyObjectPool<>(cfg.getObjectPoolSize(), OdinCacheEvent.class);

        // định kỳ xóa item quá hạn trong cache
        _remove_expiry_cache_executor.scheduleAtFixedRate(
                () -> _odin_cache_control.removeExpiry(_cfg.getOdinCacheExpiryTime(), _object_pool::push),
                3000, _cfg.getOdinCacheExpiryTime() / 2, TimeUnit.MILLISECONDS);

        // chạy luồng chính
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Odin ALL",
                true,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                this::_initMainFlow));
    }


    private void _initMainFlow() {
        log.info("Odin run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo disruptor
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Odin Disruptor",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableDisruptorBindingCore(),
                _cfg.getDisruptorCpu(),
                this::_initDisruptorCore));

        // lắng nghe khi 1 sink req loss msg
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Odin Handle Confirm",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableHandleConfirmBindingCore(),
                _cfg.getHandleConfirmCpu(),
                () -> new Thread(this::_initHandlerConfirm).start()));

        // chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    // chuyển dữ liệu sang binary và lưu vào cache
    private void numberingAndSerialize(OdinDisruptorEvent event, long sequence, boolean endOfBatch) {
        try {
            _seq++;

            // convert dữ liệu sang binary [version][seq][data_length][data]
            event.getData().writeMarshallable(_wire_temp_disruptor);
            _byte_disruptor.writeLong(_version);
            _byte_disruptor.writeLong(_seq);
            _byte_disruptor.writeInt((int) _byte_temp_disruptor.writePosition());
            _byte_disruptor.write(_byte_temp_disruptor);
            event.setBinary(_byte_disruptor.toByteArray());

            // lấy object từ pool và lưu vào cache
            OdinCacheEvent cacheEvent = _object_pool.pop();
            cacheEvent.setTime(System.currentTimeMillis());
            cacheEvent.setSeq(_seq);
            cacheEvent.setData(event.getBinary());
            _odin_cache_control.add(cacheEvent);
        } catch (Exception ex) {
            log.error("Odin.numberingAndSerialize error, event {}", event.toString(), ex);
        } finally {
            _byte_disruptor.clear();
            _byte_temp_disruptor.clear();
            _wire_temp_disruptor.clear();
        }
    }


    private void _initDisruptorCore() {
        log.info("Odin run disruptor on logical processor {}", Affinity.getCpu());

        _disruptor = new Disruptor<>(
                this::_eventFactory,
                _cfg.getRingBufferSize(),
                new OdinThreadProcessor(_cfg),
                ProducerType.MULTI,
                _cfg.getDisruptorWaitStrategy());
        _ring_buffer = _disruptor.getRingBuffer();

        EventHandler<OdinDisruptorEvent> numberingAndSerializeHandler = this::numberingAndSerialize;

        // serialize data
        _disruptor.handleEventsWith(numberingAndSerializeHandler);

        // send to Artemis
        SequenceBarrier barrier = _disruptor.after(numberingAndSerializeHandler).asSequenceBarrier();
        for (int port : _cfg.getRealtimePorts()) {
            OdinProcessor odinProcessor = new OdinProcessor(
                    _ring_buffer,
                    barrier,
                    _zmq_context,
                    port,
                    _cfg);
            _disruptor.handleEventsWith(odinProcessor);
        }

        _disruptor.start();
    }


    /**
     * lắng nghe các yêu cầu từ Artemis muốn lấy các msg bị miss
     * cấu trúc msg gồm 2 phần ["kiểu lấy dữ liệu"]["sequence from"]["sequence to"]
     * "sequence from": là sequence của msg đầu tiên bị miss
     * "sequence to": là sequence liền mạch của msg cuối cùng bị miss
     * ví dụ có [1,2,3,4,5,6], tôi muốn lấy [3,4,5] thì cần gửi "sequence from": 3, "sequence to": 5
     * dữ liệu ở đây sẽ được trả theo 2 kiểu tùy TYPE
     * kiểu 1 cho "LATEST_MSG": [version_1][seq_1][data_length_1][data_1]
     * kiểu 2 cho các loại còn lại: [version_1][seq_1][data_length_1][data_1][version_2][seq_2][data_length_2][data_2]
     */
    private void _initHandlerConfirm() {
        log.info("Odin run handle confirm on logical processor {}", Affinity.getCpu());

        ZMQ.Socket repSocket = _zmq_context.createSocket(SocketType.REP);
        repSocket.bind("tcp://*:" + _cfg.getConfirmPort());

        byte type;
        long indexFrom, indexTo;
        byte[] request;
        Bytes<ByteBuffer> byteInput = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> byteReplies = Bytes.elasticByteBuffer();

        try {
            while (_status.get() == RUNNING) {
                request = repSocket.recv(0);
                byteInput.write(request);
                type = byteInput.readByte();

                if (type == Constance.ODIN.CONFIRM.LATEST_MSG) {
                    repSocket.send(_odin_cache_control.getLatestEventData(), 0);
                } else {
                    indexFrom = byteInput.readLong();
                    indexTo = byteInput.readLong();

                    _odin_cache_control.findBetweenSequence(indexFrom, indexTo, byteReplies);
                    repSocket.send(byteReplies.toByteArray(), 0);
                    byteReplies.clear();
                }
                byteInput.clear();
            }
        } catch (Exception ex) {
            log.error("Odin handle confirm request error", ex);
        } finally {
            repSocket.close();
            byteInput.releaseLast();
            byteReplies.releaseLast();
            log.info("Odin closing listen request confirm");
        }
    }


    public boolean send(T event) {
        try {
            if (_status.get() != RUNNING) return false;
            _ring_buffer.publishEvent((newEvent, sequence, srcEvent) -> srcEvent.copyTo(newEvent.getData()), event);
            return true;
        } catch (Exception ex) {
            log.error("Odin send error, event {}", event.toString(), ex);
            return false;
        }
    }


    // Tạo một instance mới của class được chỉ định
    private OdinDisruptorEvent _eventFactory() {
        try {
            OdinDisruptorEvent event = new OdinDisruptorEvent();
            event.setData(_dataType.newInstance());
            return event;
        } catch (Exception ex) {
            log.error("Fanout _eventFactory error", ex);
            return null;
        }
    }


    public void shutdown() {
        log.info("Odin closing...");

        _status.set(STOPPED);
        LockSupport.parkNanos(1_000_000_000);

        // stop --> chờ để xử lý nốt msg
        _disruptor.shutdown();
        LockSupport.parkNanos(500_000_000);

        // close zeromq. Các socket sẽ được đóng lại cùng
        _zmq_context.destroy();

        // giải phóng CPU core/Logical processor
        for (AffinityCompose affinityCompose : _affinity_composes)
            affinityCompose.release();

        _remove_expiry_cache_executor.shutdownNow();
        _object_pool.clear();
        _byte_disruptor.releaseLast();
        _byte_temp_disruptor.releaseLast();

        LockSupport.parkNanos(500_000_000);

        log.info("Odin CLOSED !");
    }

}
