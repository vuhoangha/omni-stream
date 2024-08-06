package io.github.vuhoangha.OneToManyStateless;

import io.github.vuhoangha.Common.*;
import io.github.vuhoangha.common.SynchronizeObjectPool;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.Bytes;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@Slf4j
public class Artemis {

    private enum ArtemisStatus {SYNCING, RUNNING, STOP}

    private Artemis.ArtemisStatus status = Artemis.ArtemisStatus.SYNCING;
    private final ArtemisConfig config;
    private final NavigableMap<Long, PendingMessage> pendingMessages = new TreeMap<>();
    private final SynchronizeObjectPool<PendingMessage> messagePool;
    private final ArtemisHandler messageHandler;
    private final Consumer<String> interruptHandler;
    private long currentSequence;
    private long dataVersion;
    private boolean isApplicationReady = false;   // ứng dụng đã sẵn sàng để xử lý các message chưa
    private final List<AffinityCompose> affinityComposes = Collections.synchronizedList(new ArrayList<>());


    public Artemis(ArtemisConfig config, ArtemisHandler messageHandler, Consumer<String> interruptHandler) {
        // validate
        Utils.checkNull(config.getSourceIP(), "Require source IP");
        Utils.checkNull(messageHandler, "Require on data");
        Utils.checkNull(interruptHandler, "Require on interrupt");

        this.config = config;
        this.messageHandler = messageHandler;
        this.interruptHandler = interruptHandler;
        messagePool = new SynchronizeObjectPool<>(new PendingMessage[config.getMessagePoolSize()], PendingMessage::new);

        affinityComposes.add(Utils.runWithThreadAffinity("Artemis run all", true, config.getCore(), config.getCpu(), config.getCore(), config.getCpu(), this::processor));
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }


    public void startRealtimeData() {
        isApplicationReady = true;
    }


    private void processor() {
        ZContext zContext = new ZContext();
        ZMQ.Socket subSocket = createSubSocket(zContext);
        ZMQ.Socket dealerSocket = createDealerSocket(zContext);
        Runnable waiter = OmniWaitStrategy.getWaiter(config.getWaitStrategy());
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();
        long latestMessageFetchInterval = config.getLatestMessageFetchInterval();
        long lostMessageScanInterval = config.getLostMessageScanInterval();
        long messageQueueTimeoutBeforeRestart = config.getMessageQueueTimeoutBeforeRestart();
        long nextLatestMessageFetchTime = System.currentTimeMillis() + latestMessageFetchInterval;
        long nextLostMessageScanTime = System.currentTimeMillis() + lostMessageScanInterval;

        // synchronize dữ liệu trước
        syncMessages(subSocket, bytes, waiter);

        // bắt đầu xử lý realtime message
        while (status == ArtemisStatus.RUNNING) {
            try {
                // lấy realtime message
                while (true) {
                    byte[] realtimeBytes = subSocket.recv(ZMQ.NOBLOCK);
                    if (realtimeBytes != null) {
                        bytes.write(realtimeBytes);
                        processMessage(bytes);
                        bytes.clear();
                    } else {
                        break;
                    }
                }

                // lấy latest_message / from_to_message
                byte[] fetchingMessageBytes = dealerSocket.recv(ZMQ.NOBLOCK);
                if (fetchingMessageBytes != null && fetchingMessageBytes.length > 0) {
                    bytes.write(fetchingMessageBytes);
                    processMessage(bytes);
                    bytes.clear();
                }

                // định kỳ request xem có message nào mới không, đề phòng trường hợp subscriber message bị loss
                if (nextLatestMessageFetchTime <= System.currentTimeMillis()) {
                    nextLatestMessageFetchTime += latestMessageFetchInterval;
                    // lấy ra sequence lớn nhất từng nhận được
                    long bigestReceivingSequence = currentSequence;
                    if (!pendingMessages.isEmpty()) {
                        bigestReceivingSequence = Math.max(bigestReceivingSequence, pendingMessages.lastKey());
                    }
                    bytes.writeLong(bigestReceivingSequence);                                    // from
                    bytes.writeLong(bigestReceivingSequence + config.getMaxMessagesPerFetch());  // to
                    dealerSocket.send(bytes.toByteArray(), 0);
                    bytes.clear();
                }

                // định kỳ quét các msg bị miss
                if (nextLostMessageScanTime <= System.currentTimeMillis()) {
                    nextLostMessageScanTime += lostMessageScanInterval;
                    if (!pendingMessages.isEmpty()) {
                        PendingMessage msg = pendingMessages.firstEntry().getValue();
                        long now = System.currentTimeMillis();
                        if (msg.expiryTime + messageQueueTimeoutBeforeRestart < now) {
                            // đã quá lâu mà không thể lấy các loss message --> cần restart để đồng bộ lại
                            status = ArtemisStatus.STOP;
                            interruptHandler.accept("Message wait so long");
                            LockSupport.parkNanos(100_000_000L);
                        } else if (msg.expiryTime < now) {
                            // lấy các loss message
                            bytes.writeLong(currentSequence + 1);
                            bytes.writeLong(msg.sequence - 1);
                            dealerSocket.send(bytes.toByteArray(), 0);
                            bytes.clear();
                        }
                    }
                }

                waiter.run();
            } catch (Exception ex) {
                log.error("Artemis processor error", ex);
                subSocket.close();
                dealerSocket.close();
                LockSupport.parkNanos(1_000_000_000L);
                subSocket = createSubSocket(zContext);
                dealerSocket = createDealerSocket(zContext);
                LockSupport.parkNanos(1_000_000_000L);
            }
        }

        subSocket.close();
        dealerSocket.close();
        zContext.destroy();
        bytes.releaseLast();
    }


    // đồng bộ dữ liệu lúc ban đầu
    private void syncMessages(ZMQ.Socket subSocket, Bytes<ByteBuffer> bytes, Runnable waiter) {
        while (status == Artemis.ArtemisStatus.SYNCING) {
            // lấy realtime message
            while (true) {
                byte[] realtimeBytes = subSocket.recv(ZMQ.NOBLOCK);
                if (realtimeBytes != null) {
                    bytes.write(realtimeBytes);
                    processMessage(bytes);
                    bytes.clear();
                } else {
                    break;
                }
            }

            // nếu ứng dụng đã sẵn sàng thì bắt đầu stream message
            if (isApplicationReady) {
                // lấy sequence có giá trị nhỏ nhất làm điểm khởi đầu
                if (!pendingMessages.isEmpty()) {
                    currentSequence = pendingMessages.firstKey() - 1;
                }
                // chuyển trạng thái
                status = ArtemisStatus.RUNNING;
                LockSupport.parkNanos(100_000_000L);
                // lấy các pending message ra để xử lý
                scanPendingMessages();
            }

            waiter.run();
        }
    }


    // xử lý một list các message liền kề có dạng [version][sequence_1][is_compressed_1][data_length_1][data_1][version][sequence_2][is_compressed_2][data_length_2][data_2]
    private void processMessage(Bytes<ByteBuffer> bytes) {

        boolean hasProcessedMessage = false;    // có message nào được xử lý thành công không?

        while (bytes.readRemaining() > 0 && status != ArtemisStatus.STOP) {
            long version = bytes.readLong();
            long sequence = bytes.readLong();
            boolean isCompress = bytes.readBoolean();
            int dataLength = bytes.readInt();

            // message version thay đổi chứng tỏ Odin đã bị restart --> cần báo cho ứng dụng biết để chạy lại
            if (dataVersion > 0 && dataVersion != version) {
                status = ArtemisStatus.STOP;
                interruptHandler.accept("Interrupt because change version");
                LockSupport.parkNanos(100_000_000L);
                return;
            }
            dataVersion = version;

            if (sequence <= currentSequence) {
                // message đã xử lý thì bỏ qua
                bytes.readSkip(dataLength);
            } else {
                PendingMessage pendingMessage = messagePool.pop().setSequence(sequence);
                if (isCompress) {
                    // giải nén data
                    byte[] compressedBytes = new byte[dataLength];
                    bytes.read(compressedBytes);
                    byte[] decompressData = Lz4Compressor.decompressData(compressedBytes);
                    pendingMessage.bytes.write(decompressData);
                } else {
                    bytes.read(pendingMessage.bytes, dataLength);
                }

                if (status == ArtemisStatus.RUNNING && sequence == currentSequence + 1) {
                    // messsage liền kề thì xử lý
                    currentSequence++;
                    messageHandler.apply(currentSequence, pendingMessage.bytes);
                    pendingMessage.bytes.clear();
                    messagePool.push(pendingMessage);
                    hasProcessedMessage = true;
                } else {
                    // đẩy message vào hàng chờ
                    pendingMessage.expiryTime = System.currentTimeMillis() + config.getMessageExpirationDuration();
                    pendingMessages.put(sequence, pendingMessage);
                }

            }
        }

        // quét trong danh sách chờ xem có message nào xử lý được không ?
        if (hasProcessedMessage) scanPendingMessages();
    }


    private void scanPendingMessages() {
        while (!pendingMessages.isEmpty() && pendingMessages.firstKey() <= currentSequence + 1) {
            long sequence = pendingMessages.firstKey();
            PendingMessage pendingMessage = pendingMessages.remove(sequence);
            if (currentSequence + 1 == pendingMessage.sequence) {
                currentSequence++;
                messageHandler.apply(currentSequence, pendingMessage.bytes);
            }
            // clear data và trả về pool
            pendingMessage.bytes.clear();
            messagePool.push(pendingMessage);
        }
    }


    private ZMQ.Socket createSubSocket(ZContext zContext) {
        ZMQ.Socket socket = zContext.createSocket(SocketType.SUB);
        socket.setRcvHWM(10_000_000);
        socket.setHeartbeatIvl(10_000);
        socket.setHeartbeatTtl(15_000);
        socket.setHeartbeatTimeout(15_000);
        socket.setReconnectIVL(10_000);
        socket.setReconnectIVLMax(10_000);
        socket.connect(config.getRealTimeUrl());
        socket.subscribe(ZMQ.SUBSCRIPTION_ALL);
        return socket;
    }

    private ZMQ.Socket createDealerSocket(ZContext zContext) {
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


    private void onShutdown() {
        log.info("Artemis closing...");

        status = ArtemisStatus.STOP;
        LockSupport.parkNanos(500_000_000L);
        pendingMessages.clear();
        messagePool.clear();

        // giải phóng các CPU core / Logical processor đã sử dụng
        affinityComposes.forEach(AffinityCompose::release);

        log.info("Artemis CLOSED !");
    }


    @Getter
    @Setter
    @Accessors(chain = true)
    static class PendingMessage {

        // số thứ tự của message trong queue
        private long sequence;

        // dữ liệu sẽ gửi cho ứng dụng
        private final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

        // thời gian tối đa mà msg nằm trong hàng chờ. Nếu lâu hơn thời gian này thì coi như các msg nằm giữa "sequence cuối cùng xử lý" và "nó" đã bị loss. Cần chủ động request lấy lại
        private long expiryTime;

    }


}
