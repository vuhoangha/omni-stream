package io.github.vuhoangha.OneToMany;

import io.github.vuhoangha.Common.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/*
 * lưu vào queue:
 *      - compress true: ["sequence"]["compress_data"]
 *      - compress false: ["sequence"]["data"]
 * realtime publish:
 *      - compress true: ["topic"]["native index"]["sequence"]["compress_data"]
 *      - compress false: ["topic"]["native index"]["sequence"]["data"]
 * latest msg:
 *      - compress true: ["topic"]["native index"]["sequence"]["compress_data"]
 *      - compress false: ["topic"]["native index"]["sequence"]["data"]
 * confirm msg:
 *      - compress true: ["độ dài dữ liệu"]["native index"]["sequence"]["compress_data"]
 *      - compress false: ["độ dài dữ liệu"]["native index"]["sequence"]["data"]
 */
@Slf4j
public class Fanout {

    // quản lý trạng thái
    private final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    // tổng số item trong queue
    private long _seq_in_queue;

    private FanoutCfg _cfg;
    private SingleChronicleQueue _queue;
    private ExcerptAppender _appender;
    Bytes<ByteBuffer> _byte_to_queue = Bytes.elasticByteBuffer();           // dữ liệu để ghi vào queue
    Bytes<ByteBuffer> _byte_event_input = Bytes.elasticByteBuffer();        // dữ liệu được đọc ra từ đối tượng muốn ghi vào queue
    private ZContext _zmq_context;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());

    public Fanout(FanoutCfg cfg) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");

        _cfg = cfg;
        _status.set(RUNNING);
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
        log.info("Fanout run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo queue
        _affinity_composes.add(Utils.runWithThreadAffinity(
                "Fanout Queue",
                false,
                _cfg.getEnableBindingCore(),
                _cfg.getCpu(),
                _cfg.getEnableQueueBindingCore(),
                _cfg.getQueueCpu(),
                this::_initQueueCore));

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


    /**
     * lắng nghe các yêu cầu từ sink muốn lấy các msg bị miss
     * cấu trúc msg gồm 2 phần ["kiểu lấy dữ liệu"]["queue index from"]["queue index to"]
     * "queue index from": là index msg ngay trước msg miss
     * "queue index to": là index msg đứng sau msg muốn lấy
     * ví dụ có [1,2,3,4,5,6], tôi muốn lấy [3,4,5] thì cần gửi "queue index from": 2, "queue index to": 6
     * dữ liệu ở đây sẽ được trả theo 2 kiểu tùy TYPE
     * kiểu 1 cho "LATEST_MSG": ["msg type"]["source native index 1"]["seq in queue 1"]["data 1"]
     * kiểu 2 cho các loại còn lại: ["độ dài data 1"]["source native index 1"]["seq in queue 1"]["data 1"]["độ dài data 2"]["source native index 2"]["seq in queue 2"]["data 2"]
     */
    private void _initHandlerConfirm() {
        log.info("Fanout run handle confirm on logical processor {}", Affinity.getCpu());

        ZMQ.Socket repSocket = _zmq_context.createSocket(SocketType.REP);
        repSocket.bind("tcp://*:" + _cfg.getConfirmPort());

        long defaultIndexTo = -1;
        long defaultNextReadIndex = -2;
        long nextReadIndex = defaultNextReadIndex;        // index msg đọc tiếp theo. Nhớ là giá trị mặc định của 'nextReadIndex' phải khác 'indexTo'
        boolean moveToIndexSuccess = false;

        Bytes<ByteBuffer> byteRead = Bytes.elasticByteBuffer();     // bytes đọc từ queue ra
        Bytes<ByteBuffer> byteInput = Bytes.elasticByteBuffer();    // bytes nhận được từ zmq
        // cấu trúc ["msg_1"]["msg_2"]...vvv. Client đọc tuần tự từng field từng msg
        Bytes<ByteBuffer> byteReplies = Bytes.elasticByteBuffer();  // bytes phản hồi cho người lấy
        byte[] emptyByte = new byte[0];

        ExcerptTailer tailer = _queue.createTailer();

        try {
            while (_status.get() == RUNNING) {
                byteInput.write(repSocket.recv(0));

                byte type = byteInput.readByte();

                if (type == Constance.FANOUT.CONFIRM.LATEST_MSG) {

                    // lấy msg cuối cùng
                    tailer.direction(TailerDirection.BACKWARD).toEnd();
                    if (tailer.readBytes(byteRead)) {

                        // vị trí hiện tại có msg
                        long nativeIndex = tailer.lastReadIndex();
                        byteReplies.writeByte(Constance.FANOUT.PUB_TOPIC.MSG);
                        byteReplies.writeLong(nativeIndex);
                        byteReplies.write(byteRead);

                        repSocket.send(byteReplies.toByteArray(), 0);
                    } else {
                        // vị trí hiện tại ko có dữ liệu
                        repSocket.send(emptyByte, 0);
                    }

                    byteRead.clear();
                    byteReplies.clear();
                } else {

                    // reset chỉ số
                    nextReadIndex = defaultNextReadIndex;
                    int count = 0;      // số lượng msg đã đọc
                    long indexFrom = byteInput.readLong();
                    long indexTo = type == Constance.FANOUT.CONFIRM.FROM_TO
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

                            // update index của msg tiếp theo
                            nextReadIndex = tailer.index();

                            // độ dài dữ liệu chính trong queue - số byte để chứa sequence
                            byteReplies.writeInt((int) byteRead.writePosition() - 8);

                            // đọc và nối thêm phần native index vào kết quả trả về
                            byteReplies.writeLong(tailer.lastReadIndex());

                            // nối phần nội dung item từ queue (bao gồm sequence và data)
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
            log.error("Fanout handle confirm request error", ex);
        } finally {
            tailer.close();
            repSocket.close();
            byteRead.releaseLast();
            byteInput.releaseLast();
            byteReplies.releaseLast();

            log.info("Fanout closing listen request confirm");
        }
    }


    public void write(WriteBytesMarshallable event) {
        try {
            _byte_to_queue.writeLong(++_seq_in_queue);

            event.writeMarshallable(_byte_event_input);

            if (_cfg.getCompress()) {
                byte[] compressedData = Lz4Compressor.compressData(_byte_event_input.toByteArray());
                _byte_to_queue.write(compressedData);
            } else {
                _byte_to_queue.write(_byte_event_input);
            }

            _appender.writeBytes(_byte_to_queue);
        } catch (Exception ex) {
            log.error("Fanout write error, event {}", event.toString(), ex);
        } finally {
            _byte_event_input.clear();
            _byte_to_queue.clear();
        }
    }

    public void write(Bytes<?> eventData) {
        try {
            _byte_to_queue.writeLong(++_seq_in_queue);

            if (_cfg.getCompress()) {
                byte[] compressedData = Lz4Compressor.compressData(eventData.toByteArray());
                _byte_to_queue.write(compressedData);
            } else {
                _byte_to_queue.write(eventData);
            }

            _appender.writeBytes(_byte_to_queue);
        } catch (Exception ex) {
            log.error("Fanout write error", ex);
        } finally {
            _byte_to_queue.clear();
        }
    }


    private void _onWriteQueue(ExcerptTailer tailer) {
        log.info("Fanout listen write queue on logical processor {}", Affinity.getCpu());

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

        // di chuyển tới bản ghi cuối cùng và lắng nghe các msg kế tiếp
        tailer.toEnd();

        while (_status.get() == RUNNING) {
            try {
                if (tailer.readBytes(byteQueueItem)) {
                    // ["topic"]["source native index"]["seq in queue"]["data"]
                    byteZmqPub.writeByte(Constance.FANOUT.PUB_TOPIC.MSG);
                    byteZmqPub.writeLong(tailer.lastReadIndex());
                    byteZmqPub.write(byteQueueItem);

                    pubSocket.send(byteZmqPub.toByteArray());

                    byteQueueItem.clear();
                    byteZmqPub.clear();
                } else {
                    waiter.run();
                }
            } catch (Exception ex) {
                byteQueueItem.clear();
                byteZmqPub.clear();
                log.error("Fanout on write queue error", ex);
            }
        }

        pubSocket.close();
        tailer.close();
        byteQueueItem.releaseLast();
        byteZmqPub.releaseLast();

        log.info("Fanout closing subscribe write queue");
    }


    public void shutdown() {
        log.info("Fanout closing...");

        _status.set(STOPPED);
        LockSupport.parkNanos(1_000_000_000);

        // close zeromq. Các socket sẽ được đóng lại cùng
        _zmq_context.destroy();

        // chronicle queue sẽ đóng và lưu lại dữ liệu vào disk
        _appender.close();
        _queue.close();

        _byte_to_queue.releaseLast();

        // giải phóng CPU core/Logical processor
        for (AffinityCompose affinityCompose : _affinity_composes)
            affinityCompose.release();

        LockSupport.parkNanos(500_000_000);

        log.info("Fanout CLOSED !");
    }

}
