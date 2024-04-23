package io.github.vuhoangha.ManyToOne;

import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.Utils;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
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
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

public class Collector<T extends SelfDescribingMarshallable> {

    private static final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private int _status = IDLE;

    private static final Logger LOGGER = LoggerFactory.getLogger(Collector.class);
    private final CollectorCfg _cfg;
    private final Class<T> _dataType;
    private final BiConsumer<T, Long> _handler;
    private final SingleChronicleQueue _queue;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());


    public Collector(CollectorCfg cfg, Class<T> dataType, BiConsumer<T, Long> handler) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");
        Utils.checkNull(cfg.getReaderName(), "Require readerName");
        Utils.checkNull(dataType, "Require dataType");
        Utils.checkNull(handler, "Require handler");

        _cfg = cfg;
        _dataType = dataType;
        _handler = handler;
        _status = RUNNING;

        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(cfg.getRollCycles())
                .build();

        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Collector ALL",
                        true,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        this::_initMainFlow));

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    /**
     * Chạy luồng chính
     */
    private void _initMainFlow() {
        LOGGER.info("Collector run Main Flow on logical processor {}", Affinity.getCpu());

        // sub queue
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Collector Queue",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableQueueBindingCore(),
                        _cfg.getQueueCpu(),
                        () -> new Thread(this::_subQueue).start()));

        // sub msg
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Collector Router",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableZRouterBindingCore(),
                        _cfg.getZRouterCpu(),
                        () -> new Thread(this::_subMsg).start()));
    }


    private void _subMsg() {
        LOGGER.info("Collector run Sub Msg on logical processor {}", Affinity.getCpu());

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

            try {
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
            } catch (Exception ex) {
                LOGGER.error("Collector Sub Msg error", ex);
            }

            // close & release
            bytesTotal.releaseLast();
            bytesData.releaseLast();
            socket.close();
            appender.close();
        }
    }


    // lắng nghe event được ghi vào queue
    private void _subQueue() {
        LOGGER.info("Collector run Sub Queue on logical processor {}", Affinity.getCpu());

        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        Wire wire = WireType.BINARY.apply(bytes);
        T objT = _eventFactory();
        Runnable waiter = OmniWaitStrategy.getWaiter(_cfg.getQueueWaitStrategy());

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

        try {
            while (_status == RUNNING) {
                if (tailer.readBytes(bytes)) {
                    // deserialize binary sang T
                    objT.readMarshallable(wire);

                    _handler.accept(objT, tailer.lastReadIndex());

                    bytes.clear();
                    wire.clear();
                } else {
                    waiter.run();
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Collector SubQueue error", ex);
        }

        bytes.releaseLast();
        tailer.close();
    }


    // Tạo một instance mới của class được chỉ định
    private T _eventFactory() {
        try {
            return _dataType.newInstance();
        } catch (Exception ex) {
            LOGGER.error("Collector EventFactory error", ex);
            return null;
        }
    }


    private void _onShutdown() {
        LOGGER.info("Collector preparing shutdown");

        _status = STOPPED;

        LockSupport.parkNanos(2_000_000_000);

        _queue.close();

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(100_000_000);

        LOGGER.info("Collector SHUTDOWN !");
    }

}
