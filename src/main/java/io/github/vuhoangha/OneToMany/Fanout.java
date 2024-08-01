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
import java.util.concurrent.locks.LockSupport;

/*
 * lưu vào queue:
 *      - compressed: ["sequence"]["compress_data"]
 *      - uncompressed: ["sequence"]["data"]
 * realtime publish:
 *      - compressed: ["native index"]["sequence"]["compress_data"]
 *      - uncompressed: ["native index"]["sequence"]["data"]
 * latest msg:
 *      - compressed: ["type"]["native index"]["sequence"]["compress_data"]
 *      - uncompressed: ["type"]["native index"]["sequence"]["data"]
 * fetch msg:
 *      - compressed: ["type"]["độ dài dữ liệu nén"]["native index"]["sequence"]["compress_data"]
 *      - uncompressed: ["type"]["độ dài dữ liệu"]["native index"]["sequence"]["data"]
 */
@Slf4j
public class Fanout {

    private boolean isRunning = true;
    private long sequence;                                                        // tổng số item trong queue
    private final FanoutConfig config;
    private final SingleChronicleQueue queue;
    private final ExcerptAppender appender;
    private final Bytes<ByteBuffer> queueBuffer = Bytes.elasticByteBuffer();      // dữ liệu để ghi vào queue
    private final Bytes<ByteBuffer> eventBuffer = Bytes.elasticByteBuffer();      // dữ liệu được đọc ra từ đối tượng muốn ghi vào queue
    private final ZContext zContext = new ZContext();
    private final List<AffinityCompose> affinityManager = Collections.synchronizedList(new ArrayList<>());
    private final byte[] emptyByte = new byte[0];

    public Fanout(FanoutConfig config) {
        Utils.checkNull(config.getQueuePath(), "Require queuePath");

        this.config = config;
        queue = SingleChronicleQueueBuilder
                .binary(config.getQueuePath())
                .rollCycle(config.getRollCycles())
                .build();
        appender = queue.acquireAppender();
        sequence = queue.entryCount();

        affinityManager.add(Utils.runWithThreadAffinity(
                "Fanout ALL", true,
                this.config.getCore(), this.config.getCpu(),
                this.config.getCore(), this.config.getCpu(),
                this::mainTask));

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    private void mainTask() {
        log.info("Fanout run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo queue
        affinityManager.add(Utils.runWithThreadAffinity(
                "Fanout queue", false,
                config.getCore(), config.getCpu(),
                config.getQueueListeningCore(), config.getQueueListeningCpu(),
                this::listenToQueue));

        // lắng nghe khi 1 sink req loss msg
        affinityManager.add(Utils.runWithThreadAffinity(
                "Fanout handle messages fetching request", false,
                config.getCore(), config.getCpu(),
                config.getMessagesFetchingCore(), config.getMessagesFetchingCpu(),
                this::handleMessagesFetchingRequest));
    }


    private void listenToQueue() {
        log.info("Fanout listen queue on logical processor {}", Affinity.getCpu());

        ExcerptTailer tailer = queue.createTailer();
        ZMQ.Socket socket = createPubSocket();
        Bytes<ByteBuffer> bytesFromQueue = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesForPub = Bytes.elasticByteBuffer();
        Runnable waiter = OmniWaitStrategy.getWaiter(config.getQueueWaitStrategy());

        // di chuyển tới bản ghi cuối cùng và lắng nghe các msg kế tiếp
        tailer.toEnd();

        while (isRunning) {
            try {
                if (tailer.readBytes(bytesFromQueue)) {
                    // ["source native index"]["seq in queue"]["data"]
                    bytesForPub.writeLong(tailer.lastReadIndex());
                    bytesForPub.write(bytesFromQueue);
                    socket.send(bytesForPub.toByteArray(), 0);
                    bytesFromQueue.clear();
                    bytesForPub.clear();
                } else {
                    waiter.run();
                }
            } catch (Exception ex) {
                bytesFromQueue.clear();
                bytesForPub.clear();
                log.error("Fanout listen queue error", ex);

                // khởi tạo lại socket cho chắc
                socket.close();
                socket = createPubSocket();
                LockSupport.parkNanos(500_000_000L);
            }
        }

        socket.close();
        tailer.close();
        bytesFromQueue.releaseLast();
        bytesForPub.releaseLast();

        log.info("Fanout closing listen queue");
    }


    /**
     * lắng nghe các yêu cầu từ Sinkin muốn lấy các msg trong 1 khoảng hoặc msg mới nhất trong queue
     * cấu trúc request msg gồm: ["kiểu lấy dữ liệu"]["queue index from"]["queue index to"]
     * "queue index from": là index msg ngay trước msg muốn lấy
     * "queue index to": là index msg đứng sau msg muốn lấy
     * ví dụ có [1,2,3,4,5,6], tôi muốn lấy [3,4,5] thì cần gửi "queue index from": 2, "queue index to": 6
     * dữ liệu ở đây sẽ được trả theo 2 kiểu tùy TYPE
     * kiểu 1 cho "LATEST_MSG": ["msg type"]["source native index"]["seq in queue"]["data"]
     * kiểu 2 cho các loại còn lại: ["msg type"]["độ dài data 1"]["source native index 1"]["seq in queue 1"]["data 1"]["độ dài data 2"]["source native index 2"]["seq in queue 2"]["data 2"]
     */
    private void handleMessagesFetchingRequest() {
        log.info("Fanout handle messages fetching request on logical processor {}", Affinity.getCpu());

        ExcerptTailer tailer = queue.createTailer();
        ZMQ.Socket socket = createRouterSocket();

        Bytes<ByteBuffer> bytesFromQueue = Bytes.elasticByteBuffer();       // bytes đọc từ queue ra
        Bytes<ByteBuffer> byteReceived = Bytes.elasticByteBuffer();         // bytes nhận được từ ZeroMQ
        Bytes<ByteBuffer> bytesReply = Bytes.elasticByteBuffer();           // bytes phản hồi cho người lấy. Cấu trúc ["msg_1"]["msg_2"]...vvv. Client đọc tuần tự từng field từng msg

        while (isRunning) {
            try {
                byte[] clientAddress = socket.recv(0);
                byte[] request = socket.recv(0);
                byteReceived.write(request);
                byte type = byteReceived.readByte();

                if (type == Constance.FANOUT.FETCH.LATEST_MSG) {
                    // lấy msg cuối cùng
                    getLatestMessage(type, tailer, socket, clientAddress, bytesFromQueue, bytesReply);
                } else {
                    // lấy msg từ from --> to
                    getMessagesFromTo(type, tailer, socket, clientAddress, byteReceived, bytesFromQueue, bytesReply);
                }

                byteReceived.clear();
            } catch (Exception ex) {
                log.error("Fanout handle messages fetching request error", ex);
            }
        }

        tailer.close();
        socket.close();
        bytesFromQueue.releaseLast();
        byteReceived.releaseLast();
        bytesReply.releaseLast();

        log.info("Fanout closing handle messages fetching request");
    }


    // lấy msg gần nhất trong queue và trả về cho client
    // ["type"]["native index"]["sequence"]["compress_data"]
    private void getLatestMessage(byte type, ExcerptTailer tailer, ZMQ.Socket socket, byte[] clientAddress, Bytes<ByteBuffer> bytesFromQueue, Bytes<ByteBuffer> bytesReply) {
        // lấy msg cuối cùng
        tailer.direction(TailerDirection.BACKWARD).toEnd();

        if (tailer.readBytes(bytesFromQueue)) {
            // vị trí hiện tại có msg
            long nativeIndex = tailer.lastReadIndex();
            bytesReply.writeByte(type);
            bytesReply.writeLong(nativeIndex);
            bytesReply.write(bytesFromQueue);
            socket.send(clientAddress, ZMQ.SNDMORE);
            socket.send(bytesReply.toByteArray(), 0);
            bytesFromQueue.clear();
            bytesReply.clear();
        } else {
            // vị trí hiện tại ko có dữ liệu
            socket.send(clientAddress, ZMQ.SNDMORE);
            socket.send(emptyByte, 0);
        }
    }


    private void getMessagesFromTo(byte type, ExcerptTailer tailer, ZMQ.Socket socket, byte[] clientAddress, Bytes<ByteBuffer> byteReceived, Bytes<ByteBuffer> bytesFromQueue, Bytes<ByteBuffer> bytesReply) {

        int readMsgCount = 0;
        long fromIndex = byteReceived.readLong();
        long toIndex = type == Constance.FANOUT.FETCH.FROM_TO ? byteReceived.readLong() : -1;
        long nextReadIndex = -1;
        boolean startMoved;

        // setup lại hướng đọc cho chuẩn và di chuyển tới vị trí sẵn sàng để bắt đầu
        tailer.direction(TailerDirection.FORWARD);
        if (fromIndex == -1) {
            // đọc từ đầu queue
            tailer.toStart();
            startMoved = true;
        } else {
            // di chuyển tới index chỉ định
            // vì khi dùng hàm "moveToIndex" thì lần đọc tiếp theo là chính bản ghi có index đó
            //      --> phải đọc trước 1 lần để tăng con trỏ đọc lên
            startMoved = tailer.moveToIndex(fromIndex);
            if (startMoved) {
                tailer.readBytes(bytesFromQueue);
                bytesFromQueue.clear();
            }
        }

        if (startMoved) {
            // nếu hợp lệ thì lấy msg gửi về
            bytesReply.writeByte(type);
            while (tailer.readBytes(bytesFromQueue)                                                  // còn msg trong queue để đọc
                    && ++readMsgCount <= config.getBatchSize()                                       // số lượng msg đã đọc chưa vượt quá giới hạn
                    && (type == Constance.FANOUT.FETCH.FROM_LATEST || nextReadIndex < toIndex)) {    // nếu là "FROM_LATEST" thì ko cần quan tâm "toIndex". Còn nếu là "FROM_TO" thì check xem đã đọc tới "toIndex" chưa

                // update index của msg tiếp theo
                nextReadIndex = tailer.index();

                // dữ liệu ghi vào queue có dạng ["seq in queue"]["data"] --> "độ dài data" = "độ dài dữ liệu trong queue" - "độ dài kiểu long của seq in queue"
                // 1 msg con trong có dạng: ["độ dài data"]["source native index"]["seq in queue"]["data"]
                bytesReply.writeInt((int) bytesFromQueue.writePosition() - 8);
                bytesReply.writeLong(tailer.lastReadIndex());
                bytesReply.write(bytesFromQueue);
                bytesFromQueue.clear();
            }
            socket.send(clientAddress, ZMQ.SNDMORE);
            socket.send(bytesReply.toByteArray(), 0);
            bytesReply.clear();
        } else {
            // nếu ko hợp lệ thì gửi về dữ liệu rỗng
            socket.send(clientAddress, ZMQ.SNDMORE);
            socket.send(emptyByte, 0);
        }
    }


    public void write(WriteBytesMarshallable event) {
        event.writeMarshallable(eventBuffer);
        write(eventBuffer);
        eventBuffer.clear();
    }

    // dữ liệu ghi vào queue có dạng ["seq in queue"]["data"]
    public void write(Bytes<?> eventData) {
        try {
            queueBuffer.writeLong(++sequence);

            if (config.getCompress()) {
                byte[] compressedData = Lz4Compressor.compressData(eventData.toByteArray());
                queueBuffer.write(compressedData);
            } else {
                queueBuffer.write(eventData);
            }

            appender.writeBytes(queueBuffer);
        } catch (Exception ex) {
            log.error("Fanout write error", ex);
        } finally {
            queueBuffer.clear();
        }
    }


    private ZMQ.Socket createRouterSocket() {
        ZMQ.Socket socket = zContext.createSocket(SocketType.ROUTER);
        socket.setSndHWM(10_000_000);
        socket.setRcvHWM(10_000_000);
        socket.setHeartbeatIvl(10_000);
        socket.setHeartbeatTtl(15_000);
        socket.setHeartbeatTimeout(15_000);
        socket.bind("tcp://*:" + config.getFetchingPort());
        return socket;
    }

    /*
     * setHeartbeatIvl: interval gửi heartbeat
     * setHeartbeatTtl: báo cho client biết sau bao lâu ko có msg bất kỳ thì client tự hiểu là connect này đã bị chết. Client có thể tạo 1 connect mới để kết nối lại
     * setHeartbeatTimeout: kể từ lúc gửi msg ping, sau 1 thời gian mà ko có msg mới nào gửi tới qua socket này thì kết nối coi như đã chết. Nó sẽ hủy kết nối này và giải phóng tài nguyên
     */
    private ZMQ.Socket createPubSocket() {
        ZMQ.Socket socket = zContext.createSocket(SocketType.PUB);
        socket.setSndHWM(10_000_000);               // Thiết lập HWM cho socket. Default = 1000
        socket.setHeartbeatIvl(10_000);
        socket.setHeartbeatTtl(15_000);
        socket.setHeartbeatTimeout(15_000);
        socket.bind("tcp://*:" + config.getRealtimePort());
        return socket;
    }


    public void shutdown() {
        log.info("Fanout closing...");

        isRunning = false;
        LockSupport.parkNanos(1_000_000_000);

        zContext.destroy();
        appender.close();
        queue.close();
        queueBuffer.releaseLast();
        eventBuffer.releaseLast();

        // giải phóng CPU core/Logical processor
        for (AffinityCompose affinityCompose : affinityManager)
            affinityCompose.release();

        log.info("Fanout CLOSED !");
    }

}
