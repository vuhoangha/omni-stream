package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.common.Promise;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.queue.*;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Anubis {

    private static final long TIMEOUT_CHECK_INTERVAL_MS = 1000;       // bao lâu check timeout một lần
    private static final int RING_BUFFER_SIZE = 2048;

    private enum Status {RUNNING, STOPPED}

    private Status status = Status.RUNNING;

    private final AnubisConfig config;
    private Disruptor<AnubisDisruptorEvent> disruptor;
    private RingBuffer<AnubisDisruptorEvent> ringBuffer;
    private final ZContext zContext;
    List<AffinityCompose> threadGroups = Collections.synchronizedList(new ArrayList<>());

    // map id của item với thời gian tối đa nó chờ bên Saraswati xác nhận
    // TODO thử xem có cấu trúc dữ liệu nào tốt hơn ko
    private final ConcurrentNavigableMap<Long, Long> messageExpiryTimes = new ConcurrentSkipListMap<>();
    // map id của item với callback để call lại khi cần
    // TODO thử xem có cấu trúc dữ liệu nào tốt hơn ko
    private final ConcurrentHashMap<Long, Promise<Boolean>> messageCallbacks = new ConcurrentHashMap<>();
    // quản lý id của các request. ID sẽ increment sau mỗi request
    private long sequenceID = System.currentTimeMillis();
    // dùng để chứa dữ liệu user gửi lên sẽ ghi tạm vào đây
    private ChronicleQueue queue;
    // bytes dùng để tổng hợp dữ liệu và ghi vào chronicle queue
    private final Bytes<ByteBuffer> cqInput = Bytes.elasticByteBuffer();


    public Anubis(AnubisConfig config) {
        // validate
        Utils.checkNull(config.getSaraswatiIP(), "Require SaraswatiIP");

        this.config = config;
        zContext = new ZContext();

        threadGroups.add(Utils.runWithThreadAffinity("Anubis start", true,
                config.getCore(), config.getCpu(),
                config.getCore(), config.getCpu(),
                this::startAnubis));

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }

    private void startAnubis() {

        log.info("Start Anubis on logical processor {}", Affinity.getCpu());

        // các msg ứng dụng gửi đi sẽ ghi vào queue trước rồi mới gửi sang Saraswati để việc gửi dữ liệu nội bộ không ảnh hưởng tới luồng xử lý của ứng dụng
        Utils.deleteFolder(config.getQueueTempPath());
        queue = ChronicleQueue
                .singleBuilder(config.getQueueTempPath())
                .rollCycle(LargeRollCycles.LARGE_DAILY)
                .storeFileListener((cycle, file) -> Utils.deleteOldFiles(config.getQueueTempPath(), config.getQueueTempTtl(), ".cq4"))
                .build();

        threadGroups.add(Utils.runWithThreadAffinity("Anubis process queue message", false,
                config.getCore(), config.getCpu(),
                config.getCoreForListenQueue(), config.getCpuForListenQueue(),
                this::processQueuedMessages));

        threadGroups.add(Utils.runWithThreadAffinity("Anubis handle incoming messages", false,
                config.getCore(), config.getCpu(),
                config.getCoreForReceiveMessage(), config.getCpuForReceiveMessage(),
                this::handleIncomingMessages));
    }


    // Gom nhiều message được ứng dụng gửi --> ghi vào queue --> đọc queue --> gửi sang Saraswati
    private void handleIncomingMessages() {
        log.info("Anubis handle incoming messages on logical processor {}", Affinity.getCpu());

        disruptor = new Disruptor<>(
                AnubisDisruptorEvent::new,
                RING_BUFFER_SIZE,
                Utils.createNonDaemonThreadFactory(),
                ProducerType.MULTI,
                config.getDisruptorWaitStrategy());

        // lắng nghe các request user gửi lên và ghi vào queue
        ExcerptAppender appender = queue.createAppender();
        disruptor.handleEventsWith((event, sequence, endOfBatch) -> {
            long reqId = ++sequenceID;
            long sendingTime = event.sendingTime;

            // đánh dấu reqId với time và callback tương ứng để control việc timeout và phản hồi cho ứng dụng
            messageExpiryTimes.put(reqId, sendingTime + config.getLocalMsgTimeout());
            messageCallbacks.put(reqId, event.callback);

            // msg gửi sang Saraswati: ["time_to_live"]["req_id"]["app_data"]
            cqInput.writeLong(sendingTime + config.getRemoteMsgTimeout());
            cqInput.writeLong(reqId);
            cqInput.write(event.bytes);
            appender.writeBytes(cqInput);
            cqInput.clear();
            event.bytes.clear();
        });

        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }


    // lắng nghe msg được ghi vào queue tạm, lấy ra và gửi sang Saraswati
    private void processQueuedMessages() {
        log.info("Anubis process queue message on logical processor {}", Affinity.getCpu());

        ZMQ.Socket socket = createSendingSocket();
        ExcerptTailer tailer = queue.createTailer();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        Runnable waiter = OmniWaitStrategy.getWaiter(config.getQueueWaitStrategy());
        long nextTimeoutCheckTime = System.currentTimeMillis() + TIMEOUT_CHECK_INTERVAL_MS;     // lần check timeout tiếp theo

        while (status == Status.RUNNING) {
            try {
                // đọc và gửi hết các msg đang chờ trong queue
                while (tailer.readBytes(bytes)) {
                    socket.send(bytes.toByteArray(), 0);
                    bytes.clear();
                }

                // xử lý hết các msg mới nhận được
                while (true) {
                    byte[] reply = socket.recv(ZMQ.NOBLOCK);
                    if (reply != null) {
                        // xóa khỏi cache --> callback về
                        long reqID = Utils.bytesToLong(reply);
                        messageExpiryTimes.remove(reqID);
                        Promise<Boolean> cb = messageCallbacks.remove(reqID);
                        if (cb != null) cb.complete(true);
                    } else {
                        break;
                    }
                }

                // xử lý hết các msg timeout
                long nowMS = System.currentTimeMillis();
                if (nextTimeoutCheckTime < nowMS) {
                    while (!messageExpiryTimes.isEmpty()) {
                        Map.Entry<Long, Long> firstEntry = messageExpiryTimes.firstEntry();
                        if (firstEntry.getValue() < nowMS) {
                            // bị timeout --> xóa khỏi cache --> callback về
                            messageExpiryTimes.remove(firstEntry.getKey());
                            Promise<Boolean> cb = messageCallbacks.remove(firstEntry.getKey());
                            cb.complete(false);
                            log.warn("Anubis send msg timeout");
                        } else {
                            // dừng tìm kiếm vì key sắp xếp tăng dần và key-value tăng tỉ lệ thuận
                            break;
                        }
                    }
                    nextTimeoutCheckTime += TIMEOUT_CHECK_INTERVAL_MS;
                }

                // cho CPU nghỉ ngơi 1 chút
                waiter.run();

            } catch (Exception ex) {
                log.error("Anubis on write queue error", ex);

                // khởi tạo lại socket
                socket.close();
                socket = createSendingSocket();
                LockSupport.parkNanos(1_000_000_000L);
            }
        }

        tailer.close();
        socket.close();
        bytes.releaseLast();
    }


    private ZMQ.Socket createSendingSocket() {
        ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
        socket.setRcvHWM(1000000);
        socket.setHeartbeatIvl(30000);
        socket.setHeartbeatTtl(45000);
        socket.setHeartbeatTimeout(45000);
        socket.setReconnectIVL(10000);
        socket.setReconnectIVLMax(10000);
        socket.connect(config.getUrl());
        return socket;
    }


    public void sendMessageAsync(WriteBytesMarshallable data, Promise<Boolean> cb) {
        try {
            // gửi sang luồng chính để gửi cho core
            ringBuffer.publishEvent((newEvent, sequence, __data, __cb, __sendingTime) -> {
                __data.writeMarshallable(newEvent.bytes);
                newEvent.callback = __cb;
                newEvent.sendingTime = __sendingTime;
            }, data, cb, System.currentTimeMillis());
        } catch (Exception ex) {
            log.error("Anubis send error, data {}", data.toString(), ex);
            cb.completeWithException(ex);
        }
    }


    public boolean sendMessage(WriteBytesMarshallable data, Promise<Boolean> cb, long timeInterval) {
        try {
            sendMessageAsync(data, cb);
            return cb.get(timeInterval);
        } catch (Exception ex) {
            return false;
        }
    }


    public boolean sendMessage(WriteBytesMarshallable data, long timeInterval) {
        Promise<Boolean> cb = new Promise<>();
        return sendMessage(data, cb, timeInterval);
    }


    public boolean sendMessage(WriteBytesMarshallable data) {
        return sendMessage(data, 1_000_000L);
    }


    private void onShutdown() {
        log.info("Anubis closing...");

        status = Status.STOPPED;

        messageExpiryTimes.clear();
        messageCallbacks.clear();

        // disruptor
        disruptor.shutdown();
        LockSupport.parkNanos(500_000_000);   // tạm ngừng để xử lý nốt msg trong ring buffer

        // giải phóng các CPU core / Logical processor đã sử dụng
        threadGroups.forEach(AffinityCompose::release);

        cqInput.releaseLast();
        zContext.destroy();
        queue.close();

        log.info("Anubis SHUTDOWN !");
    }

}
