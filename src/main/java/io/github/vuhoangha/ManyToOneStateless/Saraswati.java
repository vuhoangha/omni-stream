package io.github.vuhoangha.ManyToOneStateless;

import io.github.vuhoangha.Common.AffinityCompose;
import io.github.vuhoangha.Common.Utils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
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
    private final Consumer<Bytes<ByteBuffer>> messageHandler;
    List<AffinityCompose> affinityManager = Collections.synchronizedList(new ArrayList<>());


    public Saraswati(SaraswatiConfig config, Consumer<Bytes<ByteBuffer>> messageHandler) {
        Utils.checkNull(messageHandler, "Require handler");
        this.config = config;
        this.messageHandler = messageHandler;

        affinityManager.add(Utils.runWithThreadAffinity("Saraswati listen Anubis", true,
                this.config.getCore(), this.config.getCpu(),
                this.config.getCore(), this.config.getCpu(),
                this::listenAnubis));
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }


    // lắng nghe dữ liệu Anubis gửi sang và gửi cho ứng dụng
    private void listenAnubis() {
        log.info("Saraswati listen Anubis on logical processor {}", Affinity.getCpu());

        try (ZContext context = new ZContext();
             ZMQ.Socket socket = context.createSocket(SocketType.ROUTER)) {
            configSocket(socket);

            Bytes<ByteBuffer> bytesReq = Bytes.elasticByteBuffer();     // tất cả dữ liệu gửi sang, cấu trúc ["time_to_live"]["req_id"]["data"]

            while (status == SaraswatiStatus.RUNNING) {
                try {
                    byte[] clientAddress = socket.recv(0);
                    byte[] request = socket.recv(0);

                    bytesReq.write(request);

                    long expiry = bytesReq.readLong();
                    long reqId = bytesReq.readLong();

                    if (expiry >= System.currentTimeMillis()) {
                        // msg còn hạn sử dụng --> gửi cho ứng dụng
                        messageHandler.accept(bytesReq);

                        // phản hồi Anubis confirm nhận được
                        socket.send(clientAddress, ZMQ.SNDMORE);
                        socket.send(Utils.longToBytes(reqId), 0);
                    } else {
                        log.warn("The message {} has expired {}", reqId, expiry);
                    }
                } catch (Exception ex) {
                    log.error("Saraswati listen Anubis error", ex);
                } finally {
                    bytesReq.clear();
                }
            }

            // close & release
            bytesReq.releaseLast();
        }
    }

    private void configSocket(ZMQ.Socket socket) {
        socket.setSndHWM(10_000_000);
        socket.setHeartbeatIvl(10_000);
        socket.setHeartbeatTtl(15_000);
        socket.setHeartbeatTimeout(15_000);
        socket.bind(config.getUrl());
    }

    private void onShutdown() {
        log.info("Saraswati preparing shutdown");
        status = SaraswatiStatus.STOPPED;
        LockSupport.parkNanos(1_500_000_000L);

        // giải phóng các CPU core / Logical processor đã sử dụng
        affinityManager.forEach(AffinityCompose::release);

        log.info("Saraswati SHUTDOWN !");
    }

}
