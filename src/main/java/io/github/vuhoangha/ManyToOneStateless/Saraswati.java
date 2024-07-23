package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.OmniWaitStrategy;
import io.github.vuhoangha.Common.Utils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.rollcycles.LargeRollCycles;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@Slf4j
public class Saraswati {

    private enum SaraswatiStatus {RUNNING, STOPPED}

    private SaraswatiStatus status = SaraswatiStatus.RUNNING;

    private final SaraswatiConfig config;
    private final Consumer<Bytes<ByteBuffer>> handler;      // gửi cho handle xử lý khi có dữ liệu
    List<AffinityCompose> affinityComposes = Collections.synchronizedList(new ArrayList<>());
    private final ChronicleQueue queue;                     // dữ liệu nhận được sẽ ghi tạm vào đây rồi mới gửi cho ứng dụng xử lý nhằm tách biệt luồng đọc và ghi. Tốc độ ghi quá nhanh mà đọc chậm cũng ko ảnh hưởng

    public Saraswati(SaraswatiConfig config, Consumer<Bytes<ByteBuffer>> handler) {

        Utils.checkNull(handler, "Require handler");

        this.config = config;
        this.handler = handler;

        Utils.deleteFolder(config.getQueueTempPath());
        queue = ChronicleQueue
                .singleBuilder(config.getQueueTempPath())
                .rollCycle(LargeRollCycles.LARGE_DAILY)
                .storeFileListener((cycle, file) -> Utils.deleteOldFiles(config.getQueueTempPath(), config.getQueueTempTtl(), ".cq4"))
                .build();

        affinityComposes.add(Utils.runWithThreadAffinity("Saraswati ALL", true,
                this.config.getCore(), this.config.getCpu(),
                this.config.getCore(), this.config.getCpu(),
                this::mainTask));

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }


    // luồng chính của ứng dụng
    private void mainTask() {

        affinityComposes.add(Utils.runWithThreadAffinity("Saraswati listen message from queue", false,
                config.getCore(), config.getCpu(),
                config.getCoreForListenQueue(), config.getCpuForListenQueue(),
                this::listenQueue));

        LockSupport.parkNanos(500_000_000L);

        affinityComposes.add(Utils.runWithThreadAffinity("Saraswati listen message from Anubis", false,
                config.getCore(), config.getCpu(),
                config.getCoreForListenAnubis(), config.getCpuForListenAnubis(),
                this::listenAnubis));
    }


    // lắng nghe dữ liệu được ghi vào queue và gửi cho ứng dụng xử lý
    private void listenQueue() {

        ExcerptTailer tailer = queue.createTailer();
        Runnable waiter = OmniWaitStrategy.getWaiter(config.getQueueWaitStrategy());
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

        while (status == SaraswatiStatus.RUNNING) {
            try {
                if (tailer.readBytes(bytes)) {
                    handler.accept(bytes);
                } else {
                    waiter.run();
                }
            } catch (Exception ex) {
                log.error("Saraswati listenQueue error", ex);
            } finally {
                bytes.clear();
            }
        }

        bytes.releaseLast();

    }


    // lắng nghe dữ liệu Anubis gửi sang và ghi vào queue
    private void listenAnubis() {
        log.info("Saraswati run Sub Msg on logical processor {}", Affinity.getCpu());

        ExcerptAppender appender = queue.createAppender();

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.setSndHWM(1000000);
            socket.setHeartbeatIvl(10000);
            socket.setHeartbeatTtl(15000);
            socket.setHeartbeatTimeout(15000);
            socket.bind(config.getUrl());

            Bytes<ByteBuffer> bytesReq = Bytes.elasticByteBuffer();     // tất cả dữ liệu gửi sang, cấu trúc ["time_to_live"]["req_id"]["data"]
            Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();    // thông tin dữ liệu gửi sang

            while (status == SaraswatiStatus.RUNNING) {
                try {
                    byte[] clientAddress = socket.recv(0);
                    byte[] request = socket.recv(0);

                    bytesReq.write(request);

                    long expiry = bytesReq.readLong();
                    long reqId = bytesReq.readLong();

                    if (expiry >= System.currentTimeMillis()) {     // msg còn hạn sử dụng

                        // đọc và ghi vào queue
                        bytesReq.read(bytesData);
                        appender.writeBytes(bytesData);

                        // phản hồi Anubis confirm nhận được
                        socket.send(clientAddress, ZMQ.SNDMORE);
                        socket.send(Utils.longToBytes(reqId), 0);
                    } else {
                        log.warn("The message {} has expired {}", reqId, expiry);
                    }
                } catch (Exception ex) {
                    log.error("Saraswati Sub Msg error", ex);
                } finally {
                    // clear
                    bytesReq.clear();
                    bytesData.clear();
                }
            }

            // close & release
            bytesReq.releaseLast();
            bytesData.releaseLast();
            socket.close();
        }
    }


    private void onShutdown() {
        log.info("Saraswati preparing shutdown");

        status = SaraswatiStatus.STOPPED;

        LockSupport.parkNanos(1_500_000_000);

        // giải phóng các CPU core / Logical processor đã sử dụng
        affinityComposes.forEach(AffinityCompose::release);

        log.info("Saraswati SHUTDOWN !");
    }

}
