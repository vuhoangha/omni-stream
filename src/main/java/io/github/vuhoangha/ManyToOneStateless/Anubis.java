package io.github.vuhoangha.ManyToOneStateless;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.ReflectionUtils;
import io.github.vuhoangha.Common.Utils;
import io.github.vuhoangha.common.IQueueNode;
import io.github.vuhoangha.common.Promise;
import io.github.vuhoangha.common.QueueHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Anubis {

    private static final long TIMEOUT_CHECK_INTERVAL_MS = 1_000;       // bao lâu check timeout một lần
    private static final int RING_BUFFER_SIZE = 2048;

    private final Disruptor<AnubisDisruptorEvent> disruptor;
    private final RingBuffer<AnubisDisruptorEvent> ringBuffer;
    private Processor processor;
    List<AffinityCompose> threadGroups = Collections.synchronizedList(new ArrayList<>());


    public Anubis(AnubisConfig config) {
        // validate
        Utils.checkNull(config.getSaraswatiIP(), "Require SaraswatiIP");

        disruptor = new Disruptor<>(
                AnubisDisruptorEvent::new,
                RING_BUFFER_SIZE,
                Utils.createNonDaemonThreadFactory(),
                ProducerType.MULTI,
                new SleepingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();
        processor = new Processor(config, ringBuffer);
        ringBuffer.addGatingSequences(processor.getSequence());     // thông báo cho Ring Buffer biết rằng Processor mới chỉ xử lý đến event này thôi, ko cho phép Publisher ghi đè lên các event lớn hơn
        disruptor.start();

        threadGroups.add(Utils.runWithThreadAffinity("Anubis start", true,
                config.getCore(), config.getCpu(),
                config.getCore(), config.getCpu(),
                () -> new Thread(processor).start()));

        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
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

        processor.halt();
        LockSupport.parkNanos(100_000_000);
        disruptor.shutdown();
        LockSupport.parkNanos(500_000_000);

        // giải phóng các CPU core / Logical processor đã sử dụng
        threadGroups.forEach(AffinityCompose::release);

        log.info("Anubis SHUTDOWN !");
    }


    /**
     * vì việc gửi, nhận msg qua socket phải nằm trên 1 thread và msg gửi từ nhiều thread của ứng dụng sẽ được gom lại bằng Lmax Disruptor
     * cũng nằm trên 1 thread nên gom hết chúng lại vào 1 processor
     */
    @Slf4j
    @Getter
    static class Processor implements Runnable {

        private boolean running = true;
        private final RingBuffer<AnubisDisruptorEvent> ringBuffer;
        private final Sequencer sequencer;
        private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);                 // sequence gần nhất trong Disruptor mà Processor này xử lý
        private final AnubisConfig config;
        private final QueueHashMap<Long, Long> messageExpiryTimes = new QueueHashMap<>(2_000_000);      // map reqId với thời gian tối đa nó chờ bên Saraswati xác nhận
        private final HashMap<Long, Promise<Boolean>> messageCallbacks = new HashMap<>();               // map reqId của item với callback để call lại cho ứng dụng sau khi xong


        public Processor(AnubisConfig config,
                         RingBuffer<AnubisDisruptorEvent> ringBuffer) {
            this.config = config;
            this.ringBuffer = ringBuffer;
            this.sequencer = ReflectionUtils.extractSequencer(ringBuffer);
        }


        @Override
        public void run() {
            log.info("Anubis run Processor on logical processor {}", Affinity.getCpu());

            Runnable waiter = OmniWaitStrategy.getWaiter(config.getWaitStrategy());
            long nextSequence = sequence.get() + 1L;
            long reqID = System.currentTimeMillis();
            Bytes<ByteBuffer> sendingBytes = Bytes.elasticByteBuffer();
            long nextTimeoutCheckTime = System.currentTimeMillis() + TIMEOUT_CHECK_INTERVAL_MS;     // lần check message timeout tiếp theo

            // khởi tạo socket
            try (ZContext context = new ZContext();
                 ZMQ.Socket socket = context.createSocket(SocketType.DEALER)) {
                setupSocket(socket);

                while (running) {
                    try {

                        // xem có msg nào cần gửi đi ko
                        long availableSequence = sequencer.getHighestPublishedSequence(nextSequence, ringBuffer.getCursor());     // lấy sequence được publish cuối cùng trong ring_buffer
                        if (nextSequence <= availableSequence) {
                            while (nextSequence <= availableSequence) {
                                AnubisDisruptorEvent event = ringBuffer.get(nextSequence);
                                sendMessage(socket, event, ++reqID, sendingBytes);
                                nextSequence++;
                            }
                            // TODO thử cập nhật nó cho từng item trong vòng lặp xem sao
                            sequence.set(availableSequence);    // đánh dấu đã xử lý tới event cuối cùng được ghi nhận
                        }

                        // check xem có nhận đc msg mới không?
                        while (true) {
                            byte[] reply = socket.recv(ZMQ.NOBLOCK);
                            if (reply != null) {
                                // phản hồi cho ứng dụng là gửi thành công, bên Saraswati nhận được
                                long successReqID = Utils.bytesToLong(reply);
                                messageExpiryTimes.remove(successReqID);
                                Promise<Boolean> cb = messageCallbacks.remove(successReqID);
                                if (cb != null) cb.complete(true);
                            } else {
                                break;
                            }
                        }

                        // check xem có msg nào timeout ko
                        long now = System.currentTimeMillis();
                        if (nextTimeoutCheckTime < now) {
                            nextTimeoutCheckTime += TIMEOUT_CHECK_INTERVAL_MS;
                            scanTimeoutMessage(now);
                        }

                        // cho CPU nghỉ ngơi 1 chút
                        waiter.run();

                    } catch (Exception ex) {
                        log.error("Anubis process messages error", ex);
                    }
                }
            } finally {
                sendingBytes.releaseLast();
            }
        }

        // gửi msg sang Saraswati
        private void sendMessage(ZMQ.Socket socket, AnubisDisruptorEvent event, long reqID, Bytes<ByteBuffer> sendingBytes) {
            long sendingTime = event.sendingTime;

            // đánh dấu reqID với time và callback tương ứng để control việc timeout và phản hồi cho ứng dụng
            messageExpiryTimes.put(reqID, sendingTime + config.getLocalMsgTimeout());
            messageCallbacks.put(reqID, event.callback);

            // msg gửi sang Saraswati: ["time_to_live"]["req_id"]["app_data"]
            sendingBytes.writeLong(sendingTime + config.getRemoteMsgTimeout());
            sendingBytes.writeLong(reqID);
            sendingBytes.write(event.bytes);

            socket.send(sendingBytes.toByteArray(), 0);
            sendingBytes.clear();
            event.bytes.clear();
        }


        // quét các message đã chờ quá lâu
        private void scanTimeoutMessage(long now) {
            IQueueNode<Long, Long> tail = messageExpiryTimes.getTail();
                while (tail != null && tail.getValue() < now) {
                    // phản hồi cho user là msg này bị timeout
                    long reqId = tail.getKey();
                    messageExpiryTimes.remove(reqId);
                    Promise<Boolean> cb = messageCallbacks.remove(reqId);
                    if (cb != null) cb.complete(false);

                    // reset tail
                    tail = messageExpiryTimes.getTail();
            }
        }


        private void setupSocket(ZMQ.Socket socket) {
            socket.setRcvHWM(1_000_000);
            socket.setHeartbeatIvl(30_000);
            socket.setHeartbeatTtl(45_000);
            socket.setHeartbeatTimeout(45_000);
            socket.setReconnectIVL(10_000);
            socket.setReconnectIVLMax(10_000);
            socket.connect(config.getUrl());
        }

        public void halt() {
            running = false;
        }

    }

}
