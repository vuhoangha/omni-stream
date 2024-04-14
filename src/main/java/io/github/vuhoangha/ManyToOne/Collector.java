package io.github.vuhoangha.ManyToOne;

import io.github.vuhoangha.Common.Utils;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
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
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class Collector<T extends SelfDescribingMarshallable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Collector.class);

    // config cho collector
    private final CollectorCfg _cfg;

    private final Class<T> _dataType;
    private final BiConsumer<T, Long> _handler;

    //region STATUS
    private static final int IDLE = 0;                          // nằm im
    private static final int RUNNING = IDLE + 1;                // đang chạy
    private static final int STOPPED = RUNNING + 1;             // đã dừng

    private int _status = IDLE;      // quản lý trạng thái hiện tại
    //endregion


    //region CHRONICLE QUEUE
    // connect tới folder chứa queue data. SingleChronicleQueue chỉ cho phép 1 người ghi cùng lúc
    private final SingleChronicleQueue _queue;
    //endregion


    public Collector(CollectorCfg cfg, Class<T> dataType, BiConsumer<T, Long> handler) throws Exception {
        _status = RUNNING;
        _cfg = cfg;
        this._dataType = dataType;
        this._handler = handler;

        // validate
        if (cfg.getQueuePath() == null)
            throw new Exception("Require queuePath");
        if (cfg.getReaderName() == null)
            throw new Exception("Require readerName");
        if (dataType == null)
            throw new Exception("Require dataType");
        if (handler == null)
            throw new Exception("Require handler");

        // set default value
        if (cfg.getPort() == null)
            cfg.setPort(5557);
        if (cfg.getStartId() == null)
            cfg.setStartId(-2l);
        if (cfg.getRollCycles() == null)
            cfg.setRollCycles(LargeRollCycles.LARGE_DAILY);

        // Chronicle queue
        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(cfg.getRollCycles())
                .build();

        // sub queue and msg
        new Thread(this::_subQueue).start();
        new Thread(this::_subMsg).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    private void _subMsg() {
        // dùng để ghi dữ liệu vào queue
        ExcerptAppender appender = _queue.acquireAppender();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.setSndHWM(1000000);
            socket.setHeartbeatIvl(10000);
            socket.setHeartbeatTtl(15000);
            socket.setHeartbeatTimeout(15000);
            socket.bind(_cfg.getUrl());

            byte[] clientAddress;
            byte[] request;
            Bytes<ByteBuffer> bytesTotal = Bytes.elasticByteBuffer();
            Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();
            long reqId;

            while (_status == RUNNING) {
                clientAddress = socket.recv(0);
                request = socket.recv(0);
                bytesTotal.write(request);

                // lưu vào queue
                reqId = bytesTotal.readLong();
                bytesTotal.read(bytesData);
                appender.writeBytes(bytesData);

                // gửi cho Snipper confirm nhận được
                socket.send(clientAddress, ZMQ.SNDMORE);
                socket.send(Utils.longToBytes(reqId), 0);

                // clear
                bytesTotal.clear();
                bytesData.clear();
            }

            // close & release
            bytesTotal.releaseLast();
            bytesData.releaseLast();
            socket.close();
        }

        // close & release
        appender.close();
    }


    // lắng nghe event được ghi vào queue
    private void _subQueue() {
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        Wire wire = WireType.BINARY.apply(bytes);
        T objT = _eventFactory();

        // tạo 1 tailer. Mặc định nó sẽ đọc từ lần cuối cùng nó đọc
        ExcerptTailer tailer = _queue.createTailer(_cfg.getReaderName());
        if (_cfg.getStartId() == -1) {
            // nếu có yêu cầu replay từ đầu queue
            tailer.toStart();
        } else if (_cfg.getStartId() >= 0) {
            // vì khi dùng hàm "moveToIndex" thì lần đọc tiếp theo là chính bản ghi có index đó
            //      --> phải đọc trước 1 lần để tăng con trỏ đọc lên
            if (tailer.moveToIndex(_cfg.getStartId())) {
                tailer.readBytes(bytes);
                bytes.clear();
            } else {
                LOGGER.error("Collection tailer fail because invalid index " + _cfg.getStartId());
            }
        }

        while (_status == RUNNING) {
            if (tailer.readBytes(bytes)) {
                // deserialize binary sang T
                objT.readMarshallable(wire);

                _handler.accept(objT, tailer.lastReadIndex());

                bytes.clear();
                wire.clear();
            } else {
                /*
                 * ở đây có 2 option
                 *      Thread.yield(): nhường CPU cho thread khác thực thi. Nếu ko có thread nào thì lại chạy tiếp Thread.yield()
                 *      LockSupport.parkNanos(1): cho CPU nghỉ ngơi 1 nanoseconds.
                 *          Thời gian nghỉ thực tế phụ thuộc vào hệ điều hành.
                 *          Linux thông thường là 60 microseconds
                 *          Do thời gian nghỉ ngơi quá nhỏ, nhân kernel trong linux phải gom các tiến trình lại để đánh thức nó dậy cùng 1 lúc
                 *          Nếu thời gian nhỏ quá, điều này có thể phản tác dụng vì CPU ko nghỉ được nhiều mà còn tốn thêm thời gian lập lịch cho Thread này
                 *          Tham khảo: https://hazelcast.com/blog/locksupport-parknanos-under-the-hood-and-the-curious-case-of-parking/
                 */
                LockSupport.parkNanos(1);
            }
        }

        bytes.releaseLast();
        tailer.close();
    }


    // Tạo một instance mới của class được chỉ định
    private T _eventFactory() {
        try {
            return _dataType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }
    }


    private void _onShutdown() {
        _status = STOPPED;

        // chờ các hành động xử lý nốt
        LockSupport.parkNanos(1_000_000_000);

        _queue.close();
    }

}
