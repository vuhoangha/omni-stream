package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.Constance;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.TailerDirection;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

public class Fanout<T extends SelfDescribingMarshallable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Fanout.class);

    //region STATUS
    private static final int IDLE = 0;                          // nằm im
    private static final int RUNNING = IDLE + 1;                // đang chạy
    private static final int STOPPED = RUNNING + 1;            // chuẩn bị dừng

    private int _status = IDLE;      // quản lý trạng thái hiện tại
    //endregion


    // cấu hình cho Fanout này
    private final FanoutCfg _cfg;


    //region CHRONICLE QUEUE
    // connect tới folder chứa queue data. SingleChronicleQueue chỉ cho phép 1 người ghi cùng lúc
    private final SingleChronicleQueue _queue;
    // dùng để ghi vào trong queue
    private final ExcerptAppender _appender;
    // tổng số item trong queue. Dùng để đánh dấu seq nữa
    private long _seq_in_queue;
    // kích thước động.Lưu thông tin data gửi vào và thứ tự trong queue
    Bytes<ByteBuffer> _byte_disruptor = Bytes.elasticByteBuffer();
    // lưu dữ liệu binary của event gửi vào
    Bytes<ByteBuffer> _byte_temp_disruptor = Bytes.elasticByteBuffer();
    // dùng để serialize 1 đối tượng ra byte[] và lưu vào "byte_temp_disruptor"
    Wire _wire_temp_disruptor = WireType.BINARY.apply(_byte_temp_disruptor);
    // kích thước động. Lưu dữ liệu từ Chronicle Queue streaming
    Bytes<ByteBuffer> _byte_stream_queue = Bytes.elasticByteBuffer();
    // kích thước động. Đọc dữ liệu từ Chronicle Queue ra
    Bytes<ByteBuffer> _byte_read_queue = Bytes.elasticByteBuffer();
    // kích thước động để chứa danh sách các msg để reply cho sink. Cấu trúc sẽ là ["msg_1"]["msg_2"]...vvv.
    // Client sẽ đọc tuần tự từng field của từng msg
    Bytes<ByteBuffer> _byte_list_reply = Bytes.elasticByteBuffer();
    // chứa thông tin req gửi lên
    Bytes<ByteBuffer> _byte_input_reply = Bytes.elasticByteBuffer();
    //endregion


    //region DISRUPTOR
    // disruptor dùng để gom message từ nhiều thread lại và xử lý trong 1 thread duy nhất
    private final Disruptor<T> _disruptor;
    private final RingBuffer<T> _ring_buffer;
    //endregion


    //region ZMQ
    private final ZContext _zmq_context;
    // một mảng byte rỗng để trả về cho client nếu ko có dữ liệu
    private final byte[] _empty_byte = new byte[0];
    // chứa dữ liệu có topic để pub/sub về cho client
    Bytes<ByteBuffer> _byte_zmq_pub = Bytes.elasticByteBuffer();
    //endregion


    public Fanout(FanoutCfg cfg, Class<T> dataType) throws Exception {
        _cfg = cfg;

        // validate
        if (cfg.getQueuePath() == null)
            throw new Exception("Require queuePath");
        if (dataType == null)
            throw new Exception("Require dataType");

        // assign default value
        if (cfg.getRealtimePort() == null)
            cfg.setRealtimePort(5555);
        if (cfg.getConfirmPort() == null)
            cfg.setConfirmPort(5556);
        if (cfg.getNumberMsgInBatch() == null)
            cfg.setNumberMsgInBatch(10000);
        if (cfg.getMaxNumberMsgInCachePub() == null)
            cfg.setMaxNumberMsgInCachePub(1000000);
        if (cfg.getVersion() == null)
            cfg.setVersion((byte) -128);
        if (cfg.getWaitStrategy() == null)
            cfg.setWaitStrategy(new BlockingWaitStrategy());
        if (cfg.getRingBufferSize() == null)
            cfg.setRingBufferSize(2 << 16);     // 131072
        if (cfg.getRollCycles() == null)
            cfg.setRollCycles(LargeRollCycles.LARGE_DAILY);

        // đánh dấu hệ thống bắt đầu chạy
        _status = RUNNING;

        /*
         * Sử dụng zeromq để gửi nhận dữ liệu giữa source <--> sink
         * zmq_context có thể sử dụng ở nhiều thread và nên chỉ có 1 cho mỗi process
         * socket chỉ nên sử dụng bởi 1 thread duy nhất để đảm bảo tính nhất quán
         */
        _zmq_context = new ZContext();

        /*
         *  tạo/connect 1 queue được lưu trữ dưới dạng binary
         *  định dạng binary nhỏ gọn hơn định dạng khác
         *  máy tính làm việc trực tiếp và có thể hiểu dữ liệu binary, ko cần chuyển đổi như các định dạng khác, ko cần lưu trữ thêm các ký tự thừa như JSON, XML..vv
         *  từ đó CPU và memory giảm đáng kể
         *  RollCycles: định kỳ bao lâu sẽ close file hiện tại, tạo 1 file mới. index_spacing = 256 (256 bản ghi mới đánh index 1 lần), index_count = 4096 (4096 index thì gom vào 1 segment)
         */
        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(_cfg.getRollCycles())
                .build();
        _appender = _queue.acquireAppender();
        _seq_in_queue = _queue.entryCount();   // lấy tổng số item trong queue
        new Thread(() -> _onWriteQueue(_queue.createTailer())).start();  // lắng nghe msg mới trong queue

        /*
         * tạo disruptor để hứng tất cả data được ghi từ nhiều thread
         * ringBufferSize:
         *   nếu để nhỏ quá, người viết sẽ phải chờ nếu ring_buffer đầy.
         *   nếu to quá thì sẽ tốn bộ nhớ vì LmaxDisruptor phải tạo trước 1 số lượng Object = ringBufferSize
         *   mục đích nhằm sử dụng lại các Object, tránh GC phải làm việc
         * Executors.newSingleThreadExecutor():
         *   dùng 1 thread duy nhất để đọc từ ring_buffer và ghi vào Chronicle Queue
         *   ko phải dạng daemon thread --> app khi bị kill sẽ chờ thread làm nốt công việc
         * YieldingWaitStrategy: ring buffer sử dụng Thread.yield() để khi rảnh rỗi thì nhường CPU cho thread khác chạy
         */
        this._disruptor = new Disruptor<>(
                () -> _eventFactory(dataType),
                _cfg.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                cfg.getWaitStrategy());
        _disruptor.handleEventsWith((event, sequence, endOfBatch) -> this._onWriteDisruptor(event));
        _disruptor.start();
        _ring_buffer = _disruptor.getRingBuffer();

        // lắng nghe khi 1 sink req loss msg
        new Thread(this::_listerReqConfirm).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    /**
     * Nhiều thread bên ngoài có thể gửi event cùng lúc để Persistent.
     * Event sẽ được xử lý bởi 1 processor duy nhất thông qua Disruptor
     * Trong lambda khi gọi 1 biến bên ngoài nó sẽ giữ 1 ref với biến đó
     * từ đó GC sẽ phải thu hồi thêm rác
     * vì vậy phải truyền "event" khi gọi "publishEvent" thay vì gọi trực tiếp
     * See more: https://lmax-exchange.github.io/disruptor/user-guide/index.html
     *
     * @param event event cần persistent
     * @return ghi vào queue có thành công hay không ?
     */
    public boolean write(T event) {
        // hệ thống chỉ nhận thêm msg khi đang "RUNNING"
        if (_status != RUNNING) return false;

        _ring_buffer.publishEvent((newEvent, sequence, srcEvent) -> srcEvent.copyTo(newEvent), event);
        return true;
    }


    // Tạo một instance mới của class được chỉ định
    private T _eventFactory(Class<T> dataType) {
        try {
            return dataType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }
    }


    /**
     * Lắng nghe event từ Disruptor
     * chuyển event sang binary và ghi vào Chronicle Queue
     * cấu trúc item trong queue: ["version"]["độ dài data"]["data"]["seq in queue"]
     *
     * @param event event cần persistent
     */
    private void _onWriteDisruptor(T event) {
        try {
            // chuyển event sang binary
            event.writeMarshallable(_wire_temp_disruptor);

            _byte_disruptor.writeByte(_cfg.getVersion());
            _byte_disruptor.writeInt((int) _byte_temp_disruptor.writePosition());
            _byte_disruptor.write(_byte_temp_disruptor);
            _byte_disruptor.writeLong(++_seq_in_queue);

            // ghi vào trong Chronicle Queue
            _appender.writeBytes(_byte_disruptor);
        } catch (Exception ex) {
            LOGGER.error("Fanout listen write disruptor " + event.toString() + " error");
            LOGGER.error("StackTrace", ex);
        } finally {
            _byte_temp_disruptor.clear();
            _wire_temp_disruptor.clear();
            _byte_disruptor.clear();
        }
    }


    /**
     * Lắng nghe msg được ghi vào queue
     * Bên sink sẽ lưu thêm native queue "index" của bên source nhằm mục đích trace lại
     * Cấu trúc gói tin gửi đi ["topic"]["version"]["độ dài data"]["data"]["seq in queue"]["source native index"]
     * dùng ZeroMQ pub/sub gửi các msg này sang các sink
     */
    private void _onWriteQueue(ExcerptTailer tailer) {
        ZMQ.Socket pubSocket = _zmq_context.createSocket(SocketType.PUB);
        pubSocket.setSndHWM(_cfg.getMaxNumberMsgInCachePub()); // Thiết lập HWM cho socket. Default = 1000

        /*
         * setHeartbeatIvl: interval gửi heartbeat
         * setHeartbeatTtl: báo cho client biết sau bao lâu ko có msg bất kỳ thì client tự hiểu là connect này đã bị chết. Client có thể tạo 1 connect mới để kết nối lại
         * setHeartbeatTimeout: kể từ lúc gửi msg ping, sau 1 thời gian mà ko có msg mới nào gửi tới qua socket này thì kết nối coi như đã chết. Nó sẽ hủy kết nối này và giải phóng tài nguyên
         */
        pubSocket.setHeartbeatIvl(10000);
        pubSocket.setHeartbeatTtl(15000);
        pubSocket.setHeartbeatTimeout(15000);

        pubSocket.bind("tcp://*:" + _cfg.getRealtimePort());

        // di chuyển tới bản ghi cuối cùng và lắng nghe các msg kế tiếp
        tailer.toEnd();

        while (_status == RUNNING) {
            if (tailer.readBytes(_byte_stream_queue)) {
                _byte_zmq_pub.writeByte(Constance.FANOUT.PUB_TOPIC.MSG);
                _byte_zmq_pub.write(_byte_stream_queue);
                _byte_zmq_pub.writeLong(tailer.lastReadIndex());

                pubSocket.send(_byte_zmq_pub.toByteArray());

                _byte_stream_queue.clear();
                _byte_zmq_pub.clear();
            } else {
                /**
                 * ở đây có 2 option
                 *      Thread.yield(): nhường CPU cho thread khác thực thi. Nếu ko có thread nào thì lại chạy tiếp Thread.yield()
                 *      LockSupport.parkNanos(1): cho CPU nghỉ ngơi 1 nanoseconds.
                 *          Thời gian nghỉ thực tế phụ thuộc vào hệ điều hành.
                 *          Linux thông thường là 60 microseconds
                 *          Do thời gian nghỉ ngơi quá nhỏ, nhân kernel trong linux phải gom các tiến trình lại để đánh thức nó dậy cùng 1 lúc
                 *          Nếu thời gian nhỏ quá, điều này có thể phản tác dụng vì CPU ko nghỉ được nhiều mà còn tốn thêm thời gian lập lịch cho Thread này
                 *          Tham khảo: https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/
                 */
                LockSupport.parkNanos(1);   // nghỉ 5ms
            }
        }

        LOGGER.info("Fanout closing subscribe write queue");

        pubSocket.close();
        tailer.close();
    }


    /**
     * lắng nghe các yêu cầu từ sink muốn lấy các msg bị miss
     * cấu trúc msg gồm 2 phần ["kiểu lấy dữ liệu"]["queue index from"]["queue index to"]
     * "queue index from": là index msg ngay trước msg miss
     * "queue index to": là index msg đứng sau msg muốn lấy
     * ví dụ có [1,2,3,4,5,6], tôi muốn lấy [3,4,5] thì tôi cần gửi "queue index from": 2, "queue index to": 6
     * dữ liệu ở đây sẽ được trả theo 2 kiểu tùy TYPE
     * kiểu 1 cho "LATEST_MSG": ["version 1"]["độ dài data 1"]["data 1"]["seq in queue 1"]["source native index 1"]
     * kiểu 2 cho các loại còn lại: ["version 1"]["độ dài data 1"]["data 1"]["seq in queue 1"]["source native index 1"]["version 2"]["độ dài data 2"]["data 2"]["seq in queue 2"]["source native index 2"]
     */
    private void _listerReqConfirm() {
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

        ExcerptTailer tailer = _queue.createTailer();

        while (_status == RUNNING) {
            request = repSocket.recv(0);
            _byte_input_reply.write(request);
            type = _byte_input_reply.readByte();

            if (type == Constance.FANOUT.CONFIRM.LATEST_MSG) {
                // lấy msg cuối cùng

                tailer.direction(TailerDirection.BACKWARD).toEnd();     // di chuyển tới cuối queue và đọc ngược
                if (tailer.readBytes(_byte_read_queue)) {
                    // vị trí hiện tại có msg
                    long nativeIndex = tailer.lastReadIndex();              // lấy index của bản ghi này
                    _byte_read_queue.writeLong(nativeIndex);
                    repSocket.send(_byte_read_queue.toByteArray(), 0);
                } else {
                    // vị trí hiện tại ko có dữ liệu
                    repSocket.send(_empty_byte, 0);
                }
                _byte_read_queue.clear();
            } else {
                // reset chỉ số

                nextReadIndex = defaultNextReadIndex;
                count = 0;
                indexFrom = _byte_input_reply.readLong();
                indexTo = type == Constance.FANOUT.CONFIRM.FROM_TO
                        ? _byte_input_reply.readLong()
                        : defaultIndexTo;

                // setup lại hướng đọc cho chuẩn
                tailer.direction(TailerDirection.FORWARD);

                if (indexFrom == -1) {
                    // nếu indexFrom = -1 nghĩa là đọc từ đầu queue
                    tailer.toStart();   // lần đọc tiếp theo sẽ là bản ghi đầu tiên
                    moveToIndexSuccess = true;
                } else {
                    // di chuyển tới index chỉ định
                    moveToIndexSuccess = tailer.moveToIndex(indexFrom);
                    // vì khi dùng hàm "moveToIndex" thì lần đọc tiếp theo là chính bản ghi có index đó
                    //      --> phải đọc trước 1 lần để tăng con trỏ đọc lên
                    if (moveToIndexSuccess) {
                        tailer.readBytes(_byte_read_queue);
                        _byte_read_queue.clear();
                    }
                }

                if (moveToIndexSuccess) {
                    // nếu hợp lệ thì lấy msg gửi về
                    while (tailer.readBytes(_byte_read_queue)        // còn msg trong queue để đọc
                            && ++count <= _cfg.getNumberMsgInBatch() // số lượng msg đã đọc chưa vượt quá giới hạn
                            && nextReadIndex != indexTo) {           // chưa chạm tới index_to. Vì trong trường hợp chỉ lấy theo giới hạn, ko có giới hạn index_to thì 2 field này luôn khác nhau rồi
                        // lấy index của msg tiếp theo
                        nextReadIndex = tailer.index();
                        // đọc và nối thêm phần native index trong queue
                        _byte_read_queue.writeLong(tailer.lastReadIndex());
                        // nối msg lẻ này vào list msg tổng có dạng byte[] trả về cho client
                        _byte_list_reply.write(_byte_read_queue);
                        // clear cho vòng lặp tiếp theo
                        _byte_read_queue.clear();
                    }
                    repSocket.send(_byte_list_reply.toByteArray(), 0);
                } else {
                    // nếu ko hợp lệ thì gửi về dữ liệu rỗng
                    repSocket.send(_empty_byte, 0);
                }

                _byte_read_queue.clear();
                _byte_list_reply.clear();
            }
            _byte_input_reply.clear();
        }

        LOGGER.info("Fanout closing listen request confirm");

        tailer.close();
        repSocket.close();
    }


    /**
     * Lần lượt shutdown và giải phóng tài nguyên theo thứ tự từ đầu vào --> đầu ra
     * các công việc đang làm dở sẽ làm nốt cho xong
     */
    public void shutdown() {
        LOGGER.info("Fanout closing...");

        // hệ thống chuẩn bị ngừng chạy
        _status = STOPPED;

        /**
         * ngừng nhận msg mới
         * disruptor sẽ xử lý nốt các msg nằm trong ring buffer
         */
        _disruptor.shutdown();
        LockSupport.parkNanos(1_000_000_000);  // chờ 2s để xử lý nốt msg

        // chronicle queue sẽ đóng và lưu lại dữ liệu vào disk
        _appender.close();
        _queue.close();

        // close zeromq. Các socket sẽ được đóng lại cùng
        _zmq_context.destroy();

        ReferenceOwner refId = ReferenceOwner.temporary("Fanout");
        _byte_stream_queue.release(refId);   // trả lại bộ nhớ đã được cấp phát
        _byte_read_queue.release(refId);   // trả lại bộ nhớ đã được cấp phát
        _byte_list_reply.release(refId);   // trả lại bộ nhớ đã được cấp phát
        _byte_zmq_pub.release(refId);
        _byte_input_reply.release(refId);
        _byte_disruptor.release(refId);   // trả lại bộ nhớ đã được cấp phát
        _byte_temp_disruptor.release(refId);   // trả lại bộ nhớ đã được cấp phát

        LOGGER.info("Fanout CLOSED !");
    }

}
