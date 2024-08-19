package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.Sequencer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.*;
import io.github.vuhoangha.common.RingHashMap;
import lombok.Getter;
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
import java.util.concurrent.locks.LockSupport;

/**
 * Đảm bảo phải gửi từ 1 người duy nhất, tuần tự nhé
 */
@Slf4j
public class Odin {

    private static final int RING_BUFFER_SIZE = 2048;

    @Getter
    private long sequence = 0;
    private boolean isRunning = true;
    private final OdinConfig config;
    private final Processor processor;
    private final Disruptor<WrapMessage> disruptor;
    private final RingBuffer<WrapMessage> ringBuffer;
    private final Bytes<ByteBuffer> dataBuffer = Bytes.elasticByteBuffer();
    private final long version = System.nanoTime();
    private final List<AffinityCompose> threadGroups = Collections.synchronizedList(new ArrayList<>());

    public Odin(OdinConfig config) {
        this.config = config;
        disruptor = new Disruptor<>(
                WrapMessage::new,
                RING_BUFFER_SIZE,
                Utils.createNonDaemonThreadFactory(),
                ProducerType.SINGLE,
                new SleepingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();
        processor = new Processor(config, ringBuffer);
        ringBuffer.addGatingSequences(processor.getSequence());     // thông báo cho Ring Buffer biết rằng Processor mới chỉ xử lý đến event này thôi, ko cho phép Publisher ghi đè lên các event lớn hơn
        disruptor.start();

        threadGroups.add(Utils.runWithThreadAffinity("Odin start", true,
                config.getCore(), config.getCpu(),
                config.getCore(), config.getCpu(),
                () -> new Thread(processor).start()));

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    /**
     * tổng hợp lại thành message hoàn chỉnh và gửi cho Artemis
     * message có dạng: [version][sequence][is_compressed][data_length][data]
     */
    public void sendMessage(WriteBytesMarshallable data) {
        if (!isRunning) return;

        try {
            // chuyển data sang dạng byte[] và nén lại nếu kích thước dữ liệu quá lớn
            data.writeMarshallable(dataBuffer);
            boolean compressed = false;
            byte[] dataBytes = dataBuffer.toByteArray();
            dataBuffer.clear();
            if (dataBytes.length >= config.getMinLengthForCompression()) {
                dataBytes = Lz4Compressor.compressData(dataBytes);
                compressed = true;
            }

            // tổng hợp lại thành message hoàn chỉnh [version][sequence][is_compressed][data_length][data]
            dataBuffer.writeLong(version);
            dataBuffer.writeLong(++sequence);
            dataBuffer.writeBoolean(compressed);
            dataBuffer.writeInt(dataBytes.length);
            dataBuffer.write(dataBytes);

            // gửi sang luồng chính
            ringBuffer.publishEvent((newEvent, sequence, __dataBytes, __sequence) -> {
                newEvent.bytes = __dataBytes;
                newEvent.sequence = __sequence;
            }, dataBuffer.toByteArray(), sequence);
            dataBuffer.clear();
        } catch (Exception ex) {
            log.error("Odin send message error, data {}", data.toString(), ex);
        }
    }


    public void shutdown() {
        log.info("Odin closing...");

        isRunning = false;
        LockSupport.parkNanos(1_000_000_000);
        disruptor.shutdown();
        LockSupport.parkNanos(500_000_000);
        processor.halt();
        dataBuffer.releaseLast();
        threadGroups.forEach(AffinityCompose::release);

        log.info("Odin CLOSED !");
    }


    /**
     * gom các message được Odin gửi đi vào 1 thread duy nhất xử lý cho dễ
     */
    @Slf4j
    @Getter
    static class Processor implements Runnable {

        private boolean running = true;
        private final RingBuffer<WrapMessage> ringBuffer;
        private final Sequencer sequencer;
        private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);                 // sequence gần nhất trong Disruptor mà Processor này xử lý
        private final OdinConfig config;
        private final RingHashMap<Long, byte[]> recentEvents;


        public Processor(OdinConfig config, RingBuffer<WrapMessage> ringBuffer) {
            this.config = config;
            this.ringBuffer = ringBuffer;
            sequencer = ReflectionUtils.extractSequencer(ringBuffer);
            recentEvents = new RingHashMap<>(config.getRecentEventsSize());
        }


        @Override
        public void run() {
            log.info("Odin run Processor on logical processor {}", Affinity.getCpu());

            Runnable waiter = OmniWaitStrategy.getWaiter(config.getWaitStrategy());
            long nextSequence = sequence.get() + 1L;
            Bytes<ByteBuffer> receivedBytes = Bytes.elasticByteBuffer();
            Bytes<ByteBuffer> sendingBytes = Bytes.elasticByteBuffer();
            ZContext zContext = new ZContext();
            ZMQ.Socket routerSocket = createRouterSocket(zContext);
            ZMQ.Socket pubSocket = createPubSocket(zContext);

            while (running) {
                try {

                    // xem có msg nào cần gửi đi ko
                    long availableSequence = sequencer.getHighestPublishedSequence(nextSequence, ringBuffer.getCursor());     // lấy sequence được publish cuối cùng trong ring_buffer
                    if (nextSequence <= availableSequence) {
                        while (nextSequence <= availableSequence) {
                            WrapMessage wrapMessage = ringBuffer.get(nextSequence);
                            recentEvents.put(wrapMessage.sequence, wrapMessage.bytes);
                            pubSocket.send(wrapMessage.bytes, 0);
                            nextSequence++;
                        }
                        sequence.set(availableSequence);    // đánh dấu đã xử lý tới event cuối cùng được ghi nhận
                    }

                    // check các yêu cầu lấy message trong khoảng
                    // ví dụ có [1,2,3,4,5,6], muốn lấy [3,4,5] thì cần gửi "sequence from": 3, "sequence to": 5
                    // dữ liệu trả về: [version][sequence_1][is_compressed_1][data_length_1][data_1][version][sequence_2][is_compressed_2][data_length_2][data_2]
                    while (true) {
                        byte[] clientAddress = routerSocket.recv(ZMQ.NOBLOCK);
                        if (clientAddress != null) {
                            // đọc thông tin request
                            byte[] request = routerSocket.recv(0);
                            receivedBytes.write(request);
                            long sequenceFrom = receivedBytes.readLong();
                            long sequenceTo = receivedBytes.readLong();
                            receivedBytes.clear();

                            if (recentEvents.getSize() > 0) {
                                // giới hạn lại sequenceFrom, sequenceTo cho chuẩn
                                sequenceFrom = Math.max(sequenceFrom, recentEvents.getTail().getKey());
                                sequenceTo = Math.min(sequenceTo, recentEvents.getHead().getKey());
                                sequenceTo = Math.min(sequenceTo, sequenceFrom + config.getMaxMessagesPerFetch());

                                // tổng hợp dữ liệu trả về
                                for (long currentSequence = sequenceFrom; currentSequence <= sequenceTo; currentSequence++) {
                                    byte[] bytes = recentEvents.getValue(currentSequence);
                                    if (bytes != null) sendingBytes.write(bytes);
                                }
                            }

                            routerSocket.send(clientAddress, ZMQ.SNDMORE);
                            routerSocket.send(sendingBytes.toByteArray(), 0);
                            sendingBytes.clear();
                        } else {
                            break;
                        }
                    }

                    // cho CPU nghỉ ngơi 1 chút
                    waiter.run();

                } catch (Exception ex) {
                    log.error("Odin process messages error", ex);
                    receivedBytes.clear();
                    sendingBytes.clear();
                    routerSocket.close();
                    pubSocket.close();
                    LockSupport.parkNanos(1_000_000_000L);
                    routerSocket = createRouterSocket(zContext);
                    pubSocket = createPubSocket(zContext);
                    LockSupport.parkNanos(1_000_000_000L);
                }
            }

            routerSocket.close();
            pubSocket.close();
            zContext.destroy();
            receivedBytes.releaseLast();
            sendingBytes.releaseLast();
        }


        private ZMQ.Socket createRouterSocket(ZContext zContext) {
            ZMQ.Socket socket = zContext.createSocket(SocketType.ROUTER);
            socket.setSndHWM(10_000_000);
            socket.setRcvHWM(10_000_000);
            socket.setHeartbeatIvl(10_000);
            socket.setHeartbeatTtl(15_000);
            socket.setHeartbeatTimeout(15_000);
            socket.bind("tcp://*:" + config.getRouterPort());
            return socket;
        }

        private ZMQ.Socket createPubSocket(ZContext zContext) {
            ZMQ.Socket socket = zContext.createSocket(SocketType.PUB);
            socket.setSndHWM(10_000_000);
            socket.setHeartbeatIvl(10_000);
            socket.setHeartbeatTtl(15_000);
            socket.setHeartbeatTimeout(15_000);
            socket.bind("tcp://*:" + config.getRealtimePort());
            return socket;
        }


        public void halt() {
            running = false;
        }

    }


    static class WrapMessage {
        public byte[] bytes;
        public long sequence;
    }


}
