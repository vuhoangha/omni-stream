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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@Slf4j
public class Saraswati {

    private final int IDLE = 0, RUNNING = 1, STOPPED = 2;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    private final SaraswatiCfg _cfg;
    private final Consumer<Bytes<ByteBuffer>> _handler;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());


    public Saraswati(SaraswatiCfg cfg, Consumer<Bytes<ByteBuffer>> handler) {
        // validate
        Utils.checkNull(handler, "Require handler");

        _cfg = cfg;
        _handler = handler;
        _status.set(RUNNING);

        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Saraswati ALL",
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
        log.info("Saraswati run Main Flow on logical processor {}", Affinity.getCpu());

        // sub msg
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Saraswati Router",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableZRouterBindingCore(),
                        _cfg.getZRouterCpu(),
                        () -> new Thread(this::_subMsg).start()));

        // start time server
        new Thread(this::_initTimeServer).start();
    }


    private void _subMsg() {
        log.info("Saraswati run Sub Msg on logical processor {}", Affinity.getCpu());

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.setSndHWM(1000000);
            socket.setHeartbeatIvl(10000);
            socket.setHeartbeatTtl(15000);
            socket.setHeartbeatTimeout(15000);
            socket.bind(_cfg.getUrl());

            byte[] clientAddress;
            byte[] request;
            Bytes<ByteBuffer> bytesReq = Bytes.elasticByteBuffer();     // tất cả dữ liệu gửi sang, cấu trúc ["time_to_live"]["req_id"]["data"]
            Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();    // thông tin dữ liệu gửi sang

            while (_status.get() == RUNNING) {
                try {
                    clientAddress = socket.recv(0);
                    request = socket.recv(0);
                    bytesReq.write(request);

                    long expiry = bytesReq.readLong();
                    long reqId = bytesReq.readLong();

                    if (expiry >= System.currentTimeMillis()) {     // msg còn hạn sử dụng
                        bytesReq.read(bytesData);

                        // phản hồi Snipper confirm nhận được
                        socket.send(clientAddress, ZMQ.SNDMORE);
                        socket.send(Utils.longToBytes(reqId), 0);

                        // gửi cho ứng dụng xử lý
                        _handler.accept(bytesData);
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


    private void _initTimeServer() {
        log.info("Saraswati run Time Server on logical processor {}", Affinity.getCpu());

        try (ZContext context = new ZContext()) {
            ZMQ.Socket socket = context.createSocket(SocketType.ROUTER);
            socket.setHeartbeatIvl(10000);
            socket.setHeartbeatTtl(15000);
            socket.setHeartbeatTimeout(15000);
            socket.bind(_cfg.getTimeUrl());

            byte[] clientAddress;
            byte[] request;
            Bytes<ByteBuffer> bytesRequest = Bytes.elasticByteBuffer();     // cấu trúc ["client_time"]
            Bytes<ByteBuffer> bytesResponse = Bytes.elasticByteBuffer();

            while (_status.get() == RUNNING) {
                try {
                    clientAddress = socket.recv(0);
                    request = socket.recv(0);
                    bytesRequest.write(request);
                    long clientTime = bytesRequest.readLong();

                    // gửi cho Snipper confirm nhận được ["client_time"]["system_time"]
                    bytesResponse.writeLong(clientTime);
                    bytesResponse.writeLong(System.currentTimeMillis());
                    socket.send(clientAddress, ZMQ.SNDMORE);
                    socket.send(bytesResponse.toByteArray(), 0);
                } catch (Exception ex) {
                    log.error("Saraswati Time Server error", ex);
                } finally {
                    bytesRequest.clear();
                    bytesResponse.clear();
                }
            }

            // close & release
            bytesRequest.releaseLast();
            bytesResponse.releaseLast();
            socket.close();
        }
    }


    private void _onShutdown() {
        log.info("Saraswati preparing shutdown");

        _status.set(STOPPED);

        LockSupport.parkNanos(1_500_000_000);

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(500_000_000);

        log.info("Saraswati SHUTDOWN !");
    }

}
