package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.Constance;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.Utils;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class Fanout<T extends SelfDescribingMarshallable> {

    // quản lý trạng thái
    private static final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private int _status = IDLE;

    // tổng số item trong queue
    private long _seq_in_queue;

    private static final Logger LOGGER = LoggerFactory.getLogger(Fanout.class);
    private final ReferenceOwner _ref_id = ReferenceOwner.temporary("Fanout");
    private FanoutCfg _cfg;
    private SingleChronicleQueue _queue;
    private ExcerptAppender _appender;
    Bytes<ByteBuffer> _byte_temp_disruptor = Bytes.elasticByteBuffer();
    Wire _wire_temp_disruptor = WireType.BINARY.apply(_byte_temp_disruptor);
    Bytes<ByteBuffer> _byte_disruptor = Bytes.elasticByteBuffer();
    private Disruptor<T> _disruptor;
    private RingBuffer<T> _ring_buffer;
    private Class<T> _data_type;
    private ZContext _zmq_context;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());

    public Fanout(FanoutCfg cfg, Class<T> dataType) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");
        Utils.checkNull(dataType, "Require dataType");

        _cfg = cfg;
        _data_type = dataType;
        _status = RUNNING;
        _zmq_context = new ZContext();

        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Fanout ALL",
                true,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                this::_initMainFlow));
    }


    private void _initMainFlow() {
        LOGGER.info("Fanout run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo queue
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Fanout Queue",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableQueueBindingCore(),
                _cfg.getQueueCpu(),
                this::_initQueueCore));

        // khởi tạo disruptor
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Fanout Disruptor",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableDisruptorBindingCore(),
                _cfg.getDisruptorCpu(),
                this::_initDisruptorCore));

        // lắng nghe khi 1 sink req loss msg
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Fanout Handle Confirm",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableHandleConfirmBindingCore(),
                _cfg.getHandleConfirmCpu(),
                () -> new Thread(this::_initHandlerConfirm).start()));

        // chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    private void _initQueueCore() {
        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(_cfg.getRollCycles())
                .build();
        _appender = _queue.acquireAppender();
        _seq_in_queue = _queue.entryCount();
        new Thread(() -> _onWriteQueue(_queue.createTailer())).start();
    }


    private void _initDisruptorCore() {
        LOGGER.info("Fanout run disruptor on logical processor {}", Affinity.getCpu());

        _disruptor = new Disruptor<>(
                () -> _eventFactory(_data_type),
                _cfg.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                _cfg.getDisruptorWaitStrategy());
        _disruptor.handleEventsWith((event, sequence, endOfBatch) -> this._onWriteDisruptor(event));
        _disruptor.start();
        _ring_buffer = _disruptor.getRingBuffer();
    }


    /**
     * lắng nghe các yêu cầu từ sink muốn lấy các msg bị miss
     * cấu trúc msg gồm 2 phần ["kiểu lấy dữ liệu"]["queue index from"]["queue index to"]
     * "queue index from": là index msg ngay trước msg miss
     * "queue index to": là index msg đứng sau msg muốn lấy
     * ví dụ có [1,2,3,4,5,6], tôi muốn lấy [3,4,5] thì cần gửi "queue index from": 2, "queue index to": 6
     * dữ liệu ở đây sẽ được trả theo 2 kiểu tùy TYPE
     * kiểu 1 cho "LATEST_MSG": ["version 1"]["độ dài data 1"]["data 1"]["seq in queue 1"]["source native index 1"]
     * kiểu 2 cho các loại còn lại: ["version 1"]["độ dài data 1"]["data 1"]["seq in queue 1"]["source native index 1"]["version 2"]["độ dài data 2"]["data 2"]["seq in queue 2"]["source native index 2"]
     */
    private void _initHandlerConfirm() {
        LOGGER.info("Fanout run handle confirm on logical processor {}", Affinity.getCpu());

        ZMQ.Socket repSocket = _zmq_context.createSocket(SocketType.REP);
        repSocket.bind("tcp://*:" + _cfg.getConfirmPort());

        long defaultIndexTo = -1;
        long defaultNextReadIndex = -2;
        byte type = -1;
        long indexFrom = -1;
        long indexTo = defaultIndexTo;
        long nextReadIndex = defaultNextReadIndex;        // index msg đọc tiếp theo. Nhớ là giá trị mặc định của 'nextReadIndex' phải khác 'indexTo'
        int count = 0;                                    // số lượng msg đã đọc
        boolean moveToIndexSuccess = false;
        byte[] request;

        Bytes<ByteBuffer> byteRead = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> byteInput = Bytes.elasticByteBuffer();
        // cấu trúc ["msg_1"]["msg_2"]...vvv. Client đọc tuần tự từng field từng msg
        Bytes<ByteBuffer> byteReplies = Bytes.elasticByteBuffer();
        byte[] emptyByte = new byte[0];

        ExcerptTailer tailer = _queue.createTailer();

        try {
            while (_status == RUNNING) {
                request = repSocket.recv(0);
                byteInput.write(request);
                type = byteInput.readByte();

                if (type == Constance.FANOUT.CONFIRM.LATEST_MSG) {
                    // lấy msg cuối cùng
                    tailer.direction(TailerDirection.BACKWARD).toEnd();
                    if (tailer.readBytes(byteRead)) {
                        // vị trí hiện tại có msg
                        long nativeIndex = tailer.lastReadIndex();
                        byteRead.writeLong(nativeIndex);
                        repSocket.send(byteRead.toByteArray(), 0);
                    } else {
                        // vị trí hiện tại ko có dữ liệu
                        repSocket.send(emptyByte, 0);
                    }
                    byteRead.clear();
                } else {
                    // reset chỉ số
                    nextReadIndex = defaultNextReadIndex;
                    count = 0;
                    indexFrom = byteInput.readLong();
                    indexTo = type == Constance.FANOUT.CONFIRM.FROM_TO
                            ? byteInput.readLong()
                            : defaultIndexTo;

                    // setup lại hướng đọc cho chuẩn
                    tailer.direction(TailerDirection.FORWARD);

                    if (indexFrom == -1) {
                        // đọc từ đầu queue
                        tailer.toStart();
                        moveToIndexSuccess = true;
                    } else {
                        // di chuyển tới index chỉ định
                        moveToIndexSuccess = tailer.moveToIndex(indexFrom);
                        // vì khi dùng hàm "moveToIndex" thì lần đọc tiếp theo là chính bản ghi có index đó
                        //      --> phải đọc trước 1 lần để tăng con trỏ đọc lên
                        if (moveToIndexSuccess) {
                            tailer.readBytes(byteRead);
                            byteRead.clear();
                        }
                    }

                    if (moveToIndexSuccess) {
                        // nếu hợp lệ thì lấy msg gửi về
                        while (tailer.readBytes(byteRead)        // còn msg trong queue để đọc
                                && ++count <= _cfg.getNumberMsgInBatch() // số lượng msg đã đọc chưa vượt quá giới hạn
                                && nextReadIndex != indexTo) {           // chưa chạm tới index_to. Vì trong trường hợp chỉ lấy theo giới hạn, ko có giới hạn index_to thì 2 field này luôn khác nhau rồi
                            // lấy index của msg tiếp theo
                            nextReadIndex = tailer.index();
                            // đọc và nối thêm phần native index trong queue
                            byteRead.writeLong(tailer.lastReadIndex());
                            // nối msg lẻ này vào list msg tổng có dạng byte[] trả về cho client
                            byteReplies.write(byteRead);
                            // clear cho vòng lặp tiếp theo
                            byteRead.clear();
                        }
                        repSocket.send(byteReplies.toByteArray(), 0);
                    } else {
                        // nếu ko hợp lệ thì gửi về dữ liệu rỗng
                        repSocket.send(emptyByte, 0);
                    }

                    byteRead.clear();
                    byteReplies.clear();
                }
                byteInput.clear();
            }
        } catch (Exception ex) {
            LOGGER.error("Fanout handle confirm request error", ex);
        } finally {
            tailer.close();
            repSocket.close();
            byteRead.release(_ref_id);
            byteInput.release(_ref_id);
            byteReplies.release(_ref_id);

            LOGGER.info("Fanout closing listen request confirm");
        }
    }


    public boolean write(T event) {
        try {
            if (_status != RUNNING) return false;
            _ring_buffer.publishEvent((newEvent, sequence, srcEvent) -> srcEvent.copyTo(newEvent), event);
            return true;
        } catch (Exception ex) {
            LOGGER.error("Fanout write error, event {}", event.toString(), ex);
            return false;
        }
    }


    // Tạo một instance mới của class được chỉ định
    private T _eventFactory(Class<T> dataType) {
        try {
            return dataType.newInstance();
        } catch (Exception ex) {
            LOGGER.error("Fanout _eventFactory error", ex);
            return null;
        }
    }


    private void _onWriteDisruptor(T event) {
        try {
            // chuyển event sang binary
            event.writeMarshallable(_wire_temp_disruptor);

            // cấu trúc item trong queue: ["version"]["độ dài data"]["data"]["seq in queue"]
            _byte_disruptor.writeByte(_cfg.getVersion());
            _byte_disruptor.writeInt((int) _byte_temp_disruptor.writePosition());
            _byte_disruptor.write(_byte_temp_disruptor);
            _byte_disruptor.writeLong(++_seq_in_queue);

            // ghi vào trong Chronicle Queue
            _appender.writeBytes(_byte_disruptor);
        } catch (Exception ex) {
            LOGGER.error("Fanout on write disruptor error, event {}", event.toString(), ex);
        } finally {
            _byte_temp_disruptor.clear();
            _wire_temp_disruptor.clear();
            _byte_disruptor.clear();
        }
    }


    private void _onWriteQueue(ExcerptTailer tailer) {
        LOGGER.info("Fanout listen write queue on logical processor {}", Affinity.getCpu());

        /*
         * setHeartbeatIvl: interval gửi heartbeat
         * setHeartbeatTtl: báo cho client biết sau bao lâu ko có msg bất kỳ thì client tự hiểu là connect này đã bị chết. Client có thể tạo 1 connect mới để kết nối lại
         * setHeartbeatTimeout: kể từ lúc gửi msg ping, sau 1 thời gian mà ko có msg mới nào gửi tới qua socket này thì kết nối coi như đã chết. Nó sẽ hủy kết nối này và giải phóng tài nguyên
         */
        ZMQ.Socket pubSocket = _zmq_context.createSocket(SocketType.PUB);
        pubSocket.setSndHWM(_cfg.getMaxNumberMsgInCachePub()); // Thiết lập HWM cho socket. Default = 1000
        pubSocket.setHeartbeatIvl(10000);
        pubSocket.setHeartbeatTtl(15000);
        pubSocket.setHeartbeatTimeout(15000);
        pubSocket.bind("tcp://*:" + _cfg.getRealtimePort());

        Bytes<ByteBuffer> byteQueueItem = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> byteZmqPub = Bytes.elasticByteBuffer();
        Runnable waiter = OmniWaitStrategy.getWaiter(_cfg.getQueueWaitStrategy());

        try {
            // di chuyển tới bản ghi cuối cùng và lắng nghe các msg kế tiếp
            tailer.toEnd();

            while (_status == RUNNING) {
                if (tailer.readBytes(byteQueueItem)) {
                    // ["topic"]["version"]["độ dài data"]["data"]["seq in queue"]["source native index"]
                    byteZmqPub.writeByte(Constance.FANOUT.PUB_TOPIC.MSG);
                    byteZmqPub.write(byteQueueItem);
                    byteZmqPub.writeLong(tailer.lastReadIndex());

                    pubSocket.send(byteZmqPub.toByteArray());

                    byteQueueItem.clear();
                    byteZmqPub.clear();
                } else {
                    waiter.run();
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Fanout on write queue error", ex);
        } finally {
            pubSocket.close();
            tailer.close();
            byteQueueItem.release(_ref_id);
            byteZmqPub.release(_ref_id);

            LOGGER.info("Fanout closing subscribe write queue");
        }
    }


    public void shutdown() {
        LOGGER.info("Fanout closing...");

        _status = STOPPED;
        LockSupport.parkNanos(1_000_000_000);

        // stop --> chờ để xử lý nốt msg
        _disruptor.shutdown();
        LockSupport.parkNanos(500_000_000);

        // close zeromq. Các socket sẽ được đóng lại cùng
        _zmq_context.destroy();

        // chronicle queue sẽ đóng và lưu lại dữ liệu vào disk
        _appender.close();
        _queue.close();

        _byte_disruptor.release(_ref_id);
        _byte_temp_disruptor.release(_ref_id);

        // giải phóng CPU core/Logical processor
        for (AffinityCompose affinityCompose : _affinity_composes)
            affinityCompose.release();

        LockSupport.parkNanos(500_000_000);

        LOGGER.info("Fanout CLOSED !");
    }

}
