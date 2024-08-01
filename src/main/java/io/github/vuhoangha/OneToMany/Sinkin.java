package io.github.vuhoangha.OneToMany;

import io.github.vuhoangha.Common.*;
import io.github.vuhoangha.common.SynchronizeObjectPool;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.locks.LockSupport;

/*
 *  lưu vào queue:
 *      - compressed: ["src native index"]["sequence"]["compress_data"]
 *      - uncompressed: ["src native index"]["sequence"]["data"]
 */
@Slf4j
public class Sinkin {

    private enum SinkStatus {SYNCING, RUNNING, STOP}

    private SinkStatus status = SinkStatus.SYNCING;                                       // quản lý trạng thái của Sinkin
    private final NavigableMap<Long, PendingMessage> pendingMessages = new TreeMap<>();   // TODO đoạn này xem xét tự viết 1 kiểu dữ liệu tương tự nhưng performance tốt hơn
    private final SynchronizeObjectPool<PendingMessage> messagePool;
    private final SinkinConfig config;
    private final SingleChronicleQueue queue;
    private final ZContext zContext = new ZContext();
    List<AffinityCompose> affinityComposes = Collections.synchronizedList(new ArrayList<>());
    private final SinkinHandler messageHandler;
    private long latestWriteSequence;                                                     // số thứ tự của msg trong queue. Được đánh bởi bên Fanout
    private long latestWriteIndex;                                                        // src index của item mới nhất trong Fanout queue
    private long lastSequenceAfterSync = -1;                                              // sequence kết thúc việc đồng bộ dữ liệu ban đầu từ Fanout


    public Sinkin(SinkinConfig config, SinkinHandler handler) {
        Utils.checkNull(config.getQueuePath(), "Require queuePath");
        Utils.checkNull(config.getSourceIP(), "Require source IP");
        Utils.checkNull(config.getReaderName(), "Require readerName");
        Utils.checkNull(handler, "Require handler");

        this.config = config;
        messageHandler = handler;
        messagePool = new SynchronizeObjectPool<>(new PendingMessage[config.getMessagePoolSize()], PendingMessage::new);
        queue = SingleChronicleQueueBuilder.binary(config.getQueuePath()).rollCycle(config.getRollCycles()).build();
        syncLatestQueueInfo();

        // chạy đồng bộ dữ liệu với Fanout trước
        new Thread(this::sync).start();
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }


    /**
     * Hàm này sẽ đồng bộ tất cả msg từ Fanout --> Sinkin. Khi đã hoàn thành thì mới subscriber message mới
     * cấu trúc request: ["type"]["src index"]
     * Cấu trúc response: ["type"]["msg_1"]["msg_2"]...vvv
     * cấu trúc từng msg con msg_1, msg_2..vv:  ["độ dài data 1]["source native index 1"]["sequence 1"]["data nén 1"]["độ dài data 2]["source native index 2"]["sequence 2"]["data nén 2"]
     */
    private void sync() {
        ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
        socket.connect(config.getDealerUrl());

        ExcerptAppender localAppender = queue.acquireAppender();
        Bytes<ByteBuffer> byteResponse = Bytes.elasticByteBuffer();             // chứa byte[] của tất cả bản ghi trả về
        Bytes<ByteBuffer> byteToQueue = Bytes.elasticByteBuffer();              // chứa byte[] để ghi vào queue
        Bytes<ByteBuffer> msgFetchingBytes = Bytes.elasticByteBuffer();         // dùng để tạo data lấy msg miss

        try {
            while (status == SinkStatus.SYNCING) {
                // tổng hợp data rồi req sang Fanout
                msgFetchingBytes.writeByte(Constance.FANOUT.FETCH.FROM_LATEST);
                msgFetchingBytes.writeLong(latestWriteIndex);
                socket.send(msgFetchingBytes.toByteArray(), 0);
                msgFetchingBytes.clear();

                // chờ dữ liệu trả về. Dữ liệu null hoặc chỉ chứa 1 byte duy nhất là "type" ở đầu chứng tỏ đã đồng bộ dữ liệu hoàn toàn
                byte[] repData = socket.recv(0);
                if (repData == null || repData.length <= 1) break;

                log.info("Sinkin syncing......");

                // đọc tuần tự và xử lý
                byteResponse.write(repData);
                byteResponse.readSkip(1);   // "type" ko cần nên bỏ qua
                while (byteResponse.readRemaining() > 0) {
                    // cấu trúc data: ["độ dài data]["source native index"]["sequence"]["data"]
                    int dataLen = byteResponse.readInt();
                    byteToQueue.write(byteResponse, byteResponse.readPosition(), 8 + 8 + dataLen);
                    long sourceIndex = byteResponse.readLong();
                    long sequence = byteResponse.readLong();
                    byteResponse.readSkip(dataLen); // bỏ qua data chính

                    if (sequence != latestWriteSequence + 1) {
                        log.error("Sinkin sync not sequence, src_seq: {}, sink_seq: {}", sequence, latestWriteSequence);
                        return;
                    }

                    latestWriteSequence = sequence;
                    latestWriteIndex = sourceIndex;
                    localAppender.writeBytes(byteToQueue);
                    byteToQueue.clear();
                }
                byteResponse.clear();
            }

            log.info("Sinkin synced");

            lastSequenceAfterSync = latestWriteSequence;
            status = SinkStatus.RUNNING;
            LockSupport.parkNanos(100_000_000L);

            // realtime message từ Fanout về
            affinityComposes.add(Utils.runWithThreadAffinity("Sinkin processor", false, false, Constance.CPU_TYPE.NONE,
                    config.getProcessorCore(), config.getProcessorCpu(), this::processor));

            // đọc từ queue và gửi cho ứng dụng
            affinityComposes.add(Utils.runWithThreadAffinity("Sinkin listen queue", false, false, Constance.CPU_TYPE.NONE,
                    config.getQueueCore(), config.getQueueCpu(), this::listenQueue));

        } catch (Exception ex) {
            log.error("Sinkin sync error", ex);
        } finally {
            // giải phóng tài nguyên
            localAppender.close();
            socket.close();
            byteResponse.releaseLast();
            byteToQueue.releaseLast();
            msgFetchingBytes.releaseLast();

            log.info("Sinkin synced done !");
        }
    }


    // chạy luồng realtime và control message từ Fanout
    private void processor() {
        ZMQ.Socket subSocket = createSubSocket();
        ZMQ.Socket dealerSocket = createDealerSocket();
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        ExcerptAppender appender = queue.acquireAppender();
        long latestMessageFetchInterval = config.getLatestMessageFetchInterval();
        long lostMessageScanInterval = config.getLostMessageScanInterval();
        long nextLatestMessageFetchTime = System.currentTimeMillis() + latestMessageFetchInterval;
        long nextLostMessageScanTime = System.currentTimeMillis() + lostMessageScanInterval;

        while (status == SinkStatus.RUNNING) {
            try {
                // lấy message realtime
                byte[] realtimeBytes = subSocket.recv(ZMQ.NOBLOCK);
                if (realtimeBytes != null) {
                    bytes.write(realtimeBytes);
                    boolean messageProcessed = processRealtimeBytes(bytes, appender);
                    if (messageProcessed) scanPendingMessages(appender);
                    bytes.clear();
                }

                // lấy latest_message / from_to_message
                byte[] fetchingMessageBytes = dealerSocket.recv(ZMQ.NOBLOCK);
                if (fetchingMessageBytes != null) {
                    bytes.write(fetchingMessageBytes);
                    byte type = bytes.readByte();
                    if (type == Constance.FANOUT.FETCH.LATEST_MSG) {
                        boolean messageProcessed = processLatestMessage(bytes, appender);
                        if (messageProcessed) scanPendingMessages(appender);
                    } else if (type == Constance.FANOUT.FETCH.FROM_TO) {
                        boolean messageProcessed = processFromToMessage(bytes, appender);
                        if (messageProcessed) scanPendingMessages(appender);
                    }
                    bytes.clear();
                }

                // định kỳ request latest message
                if (nextLatestMessageFetchTime <= System.currentTimeMillis()) {
                    nextLatestMessageFetchTime += latestMessageFetchInterval;
                    bytes.writeByte(Constance.FANOUT.FETCH.LATEST_MSG);
                    dealerSocket.send(bytes.toByteArray(), 0);
                    bytes.clear();
                }

                // định kỳ quét các msg bị miss
                if (nextLostMessageScanTime <= System.currentTimeMillis()) {
                    nextLostMessageScanTime += lostMessageScanInterval;
                    if (!pendingMessages.isEmpty()) {
                        PendingMessage msg = pendingMessages.firstEntry().getValue();
                        if (msg.expiryTime < System.currentTimeMillis()) {
                            bytes.writeByte(Constance.FANOUT.FETCH.FROM_TO);
                            bytes.writeLong(latestWriteIndex);
                            bytes.writeLong(msg.sourceIndex);
                            dealerSocket.send(bytes.toByteArray(), 0);
                            bytes.clear();
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Sinkin processor error", e);

                // khởi tạo lại socket cho chắc
                subSocket.close();
                dealerSocket.close();
                subSocket = createSubSocket();
                dealerSocket = createDealerSocket();
                LockSupport.parkNanos(2_000_000_000L);
            }
        }

        subSocket.close();
        dealerSocket.close();
        appender.close();
        bytes.releaseLast();
    }


    // dữ liệu dạng: ["source index"]["sequence"]["data"]
    private boolean processRealtimeBytes(Bytes<ByteBuffer> bytes, ExcerptAppender appender) {
        long sourceIndex = bytes.readLong();
        long sequence = bytes.readLong();
        bytes.readPosition(0);  // reset lại con trỏ đọc từ đầu

        if (sequence <= latestWriteSequence) {                // message đã xử lý thì bỏ qua
            return false;
        } else if (sequence == latestWriteSequence + 1) {      // message kế tiếp thì xử lý
            latestWriteSequence = sequence;
            latestWriteIndex = sourceIndex;
            appender.writeBytes(bytes);
            return true;
        } else {                                              // "message > current + 1" thì đẩy vào hàng chờ
            PendingMessage pendingMessage = messagePool.pop();
            pendingMessage.sequence = sequence;
            pendingMessage.sourceIndex = sourceIndex;
            pendingMessage.queueBytes.write(bytes);
            pendingMessage.expiryTime = System.currentTimeMillis() + config.getMessageExpirationDuration();
            pendingMessages.put(sequence, pendingMessage);
            return false;
        }
    }


    // dữ liệu dạng: ["type"]["source index"]["sequence"]["data"]
    // bên ngoài đã đọc "type" của nó nên read_position đang đứng ở "type" rồi
    private boolean processLatestMessage(Bytes<ByteBuffer> bytes, ExcerptAppender appender) {
        long sourceIndex = bytes.readLong();
        long sequence = bytes.readLong();
        bytes.readPosition(1);  // reset con trỏ đọc về vị trí "type"

        if (sequence <= latestWriteSequence) {                // message đã xử lý thì bỏ qua
            return false;
        } else if (sequence == latestWriteSequence + 1) {     // message kế tiếp thì xử lý
            latestWriteSequence = sequence;
            latestWriteIndex = sourceIndex;
            appender.writeBytes(bytes);
            return true;
        } else {                                              // "message > current + 1" thì đẩy vào hàng chờ
            PendingMessage pendingMessage = messagePool.pop();
            pendingMessage.sequence = sequence;
            pendingMessage.sourceIndex = sourceIndex;
            pendingMessage.queueBytes.write(bytes);
            pendingMessage.expiryTime = System.currentTimeMillis() + config.getMessageExpirationDuration();
            pendingMessages.put(sequence, pendingMessage);
            return false;
        }
    }


    // ["msg type"]["độ dài data 1"]["source native index 1"]["seq in queue 1"]["data 1"]["độ dài data 2"]["source native index 2"]["seq in queue 2"]["data 2"]
    private boolean processFromToMessage(Bytes<ByteBuffer> bytes, ExcerptAppender appender) {
        int messageProcessed = 0;

        while (bytes.readRemaining() > 0) {
            long oldReadPosition = bytes.readPosition();
            long oldReadLimit = bytes.readLimit();
            int dataLength = bytes.readInt();
            long sourceIndex = bytes.readLong();
            long sequence = bytes.readLong();

            // giới hạn data có thể đọc của bytes
            bytes.readPosition(oldReadPosition + 4);
            bytes.readLimit(oldReadPosition + 4 + 8 + 8 + dataLength);

            if (sequence == latestWriteSequence + 1) {
                // message kế tiếp thì xử lý
                latestWriteSequence = sequence;
                latestWriteIndex = sourceIndex;
                appender.writeBytes(bytes);
                messageProcessed++;
            } else if (sequence > latestWriteSequence + 1) {
                // "message > current + 1" thì đẩy vào hàng chờ
                PendingMessage pendingMessage = messagePool.pop();
                pendingMessage.sequence = sequence;
                pendingMessage.sourceIndex = sourceIndex;
                pendingMessage.queueBytes.write(bytes);
                pendingMessage.expiryTime = System.currentTimeMillis() + config.getMessageExpirationDuration();
                pendingMessages.put(sequence, pendingMessage);
            }

            // trả lại read_position và read_limit chính xác cho "bytes" để xử lý message tiếp theo
            bytes.readPosition(oldReadPosition + 4 + 8 + 8 + dataLength);
            bytes.readLimit(oldReadLimit);
        }

        return messageProcessed > 0;
    }


    // quét xem có message nào hợp lệ để xử lý trong danh sách message chờ không ?
    private void scanPendingMessages(ExcerptAppender appender) {
        while (!pendingMessages.isEmpty() && pendingMessages.firstKey() <= latestWriteSequence + 1) {
            long currentSequence = pendingMessages.firstKey();
            PendingMessage message = pendingMessages.remove(currentSequence);
            if (currentSequence == latestWriteSequence + 1) {
                latestWriteSequence = message.sequence;
                latestWriteIndex = message.sourceIndex;
                appender.writeBytes(message.queueBytes);
            }

            // clear data và trả về pool
            message.queueBytes.clear();
            messagePool.push(message);
        }
    }


    private ZMQ.Socket createSubSocket() {
        ZMQ.Socket subSocket = zContext.createSocket(SocketType.SUB);
        subSocket.setRcvHWM(config.getZmqSubBufferSize());   // setting buffer size các msg được nhận

        /*
         * setHeartbeatIvl: interval gửi heartbeat
         * setHeartbeatTtl: đoạn này có vẻ dùng cho server hơn (config này được gửi cho client, để client biết được sau bao lâu ko có msg bất kỳ thì client tự hiểu là connect này đã bị chết. Client có thể tạo 1 connect mới để kết nối lại)
         * setHeartbeatTimeout: kể từ lúc gửi msg ping, sau 1 thời gian nhất định mà ko có msg mới nào thì đánh dấu kết nối tới server đã chết
         * setReconnectIVL: interval time reconnect lại nếu connect tới server gặp lỗi
         * setReconnectIVLMax: trong zmq, sau mỗi lần reconnect ko thành công, nó sẽ x2 thời gian chờ lên và connect lại. Giá trị sau khi x2 cũng ko vượt quá "setReconnectIVLMax"
         */
        subSocket.setHeartbeatIvl(10_000);
        subSocket.setHeartbeatTtl(15_000);
        subSocket.setHeartbeatTimeout(15_000);
        subSocket.setReconnectIVL(10_000);
        subSocket.setReconnectIVLMax(10_000);

        subSocket.connect(config.getRealTimeUrl());
        subSocket.subscribe(ZMQ.SUBSCRIPTION_ALL);     // nhận tất cả tin nhắn từ publisher

        return subSocket;
    }

    private ZMQ.Socket createDealerSocket() {
        ZMQ.Socket socket = zContext.createSocket(SocketType.DEALER);
        socket.setRcvHWM(10_000_000);
        socket.setHeartbeatIvl(30_000);
        socket.setHeartbeatTtl(45_000);
        socket.setHeartbeatTimeout(45_000);
        socket.setReconnectIVL(10_000);
        socket.setReconnectIVLMax(10_000);
        socket.connect(config.getDealerUrl());
        return socket;
    }


    // lấy các chỉ số sequence và source native index của message cuối cùng
    private void syncLatestQueueInfo() {
        try {
            if (queue.lastIndex() >= 0) {
                Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
                ExcerptTailer tailer = queue.createTailer();
                tailer.moveToIndex(queue.lastIndex());
                tailer.readBytes(bytes);

                latestWriteIndex = bytes.readLong();
                latestWriteSequence = bytes.readLong();
                if (latestWriteSequence != queue.entryCount()) {
                    throw new RuntimeException(MessageFormat.format("Sequence in queue not match, latestWriteSequence {0}, entryCount {1}", latestWriteSequence, queue.entryCount()));
                }
                bytes.releaseLast();
            } else {
                latestWriteIndex = -1;
                latestWriteSequence = 0;
            }
        } catch (Exception ex) {
            log.error("Sinkin sync latest queue info error", ex);
            throw new RuntimeException(ex);
        }
    }


    // gửi cho ứng dụng nếu có message mới
    private void listenQueue() {
        log.info("Sinkin listen queue on logical processor {}", Affinity.getCpu());

        Bytes<ByteBuffer> bytesRead = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesDecompress = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();

        // di chuyển con trỏ đọc về nơi mong muốn
        ExcerptTailer tailer = queue.createTailer(config.getReaderName());
        if (config.getStartId() == -1) {
            // nếu có yêu cầu replay từ đầu queue
            tailer.toStart();
        } else if (config.getStartId() >= 0) {
            // vì khi dùng hàm "moveToIndex" thì lần đọc tiếp theo là chính bản ghi có index đó
            //      --> phải đọc trước 1 lần để tăng con trỏ đọc lên
            if (tailer.moveToIndex(config.getStartId())) {
                tailer.readBytes(bytesRead);
                bytesRead.clear();
            } else {
                log.error("Sinkin tailer fail because invalid index {}", config.getStartId());
            }
        }

        Runnable waiter = OmniWaitStrategy.getWaiter(config.getQueueWaitStrategy());
        while (status == SinkStatus.RUNNING) {
            try {
                if (tailer.readBytes(bytesRead)) {
                    bytesRead.readSkip(8);  // bỏ qua src index
                    long sequence = bytesRead.readLong();

                    if (config.getCompress()) {
                        // dữ liệu bị nén
                        byte[] compressedBytes = new byte[(int) bytesRead.readRemaining()];
                        bytesRead.read(compressedBytes);
                        byte[] decompressData = Lz4Compressor.decompressData(compressedBytes);
                        bytesDecompress.write(decompressData);
                        messageHandler.apply(tailer.lastReadIndex(), sequence, lastSequenceAfterSync, bytesDecompress);
                    } else {
                        // dữ liệu ko bị nén
                        bytesRead.read(bytesData);
                        messageHandler.apply(tailer.lastReadIndex(), sequence, lastSequenceAfterSync, bytesData);
                    }

                    bytesRead.clear();
                    bytesData.clear();
                    bytesDecompress.clear();
                } else {
                    waiter.run();
                }
            } catch (Exception ex) {
                bytesRead.clear();
                bytesData.clear();
                bytesDecompress.clear();

                log.error("Sinkin listen queue error", ex);
            }
        }

        bytesRead.releaseLast();
        bytesData.releaseLast();
        bytesDecompress.releaseLast();
        tailer.close();

        log.info("Sinkin listen queue closed");
    }


    /**
     * Lần lượt shutdown và giải phóng tài nguyên theo thứ tự từ đầu vào --> đầu ra
     * các công việc đang làm dở sẽ làm nốt cho xong
     */
    private void onShutdown() {
        log.info("Sinkin closing...");

        status = SinkStatus.STOP;
        zContext.destroy(); // close zeromq, ngừng nhận msg mới
        LockSupport.parkNanos(1_000_000_000);
        queue.close();
        messagePool.clear();

        // giải phóng các CPU core / Logical processor đã sử dụng
        affinityComposes.forEach(AffinityCompose::release);

        log.info("Sinkin CLOSED !");
    }


    @Getter
    @Setter
    static class PendingMessage {

        // số thứ tự của message trong queue
        private long sequence;

        // index trong native queue ở src
        private long sourceIndex;

        // thời gian tối đa mà msg nằm trong hàng chờ. Nếu lâu hơn thời gian này thì coi như các msg nằm giữa "sequence cuối cùng xử lý" và "nó" đã bị loss. Cần chủ động request lấy lại
        private long expiryTime;

        // dữ liệu dùng để ghi thẳng vào queue. Cấu trúc ["source native index"]["seq in queue"]["data"]
        private final Bytes<ByteBuffer> queueBytes = Bytes.elasticByteBuffer();

    }

}
