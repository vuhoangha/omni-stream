package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

@Slf4j
public class Artemis {

    private final int IDLE = 0, STARTING = 1, RUNNING = 2, STOP = 3;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    ScheduledExecutorService _extor_check_msg = Executors.newScheduledThreadPool(1);
    private final NavigableMap<Long, ArtemisCacheMsg> _msg_wait = new TreeMap<>();      // môi trường đơn luồng
    private final ObjectPool<ArtemisCacheMsg> _object_pool;     // môi trường đơn luồng
    private final ArtemisHandler _onData;
    private final Consumer<String> _onInterrupt;
    private final Consumer<String> _onWarning;
    private final ArtemisCfg _cfg;
    private long _seq;              // sequence của msg từ ODIN
    private long _version;          // version của msg từ ODIN
    private final ZContext _zmq_context;
    private final Bytes<ByteBuffer> _byte_miss_msg = Bytes.elasticByteBuffer();     // dùng để tạo data lấy msg miss
    private byte[] _byte_miss_msg_raw;                                              // dùng để lấy msg miss raw
    private Disruptor<ArtemisProcessMsg> _disruptor_process_msg;
    private RingBuffer<ArtemisProcessMsg> _ring_buffer_process_msg;
    private Disruptor<CheckMissMsgStateless> _disruptor_miss_msg;
    private RingBuffer<CheckMissMsgStateless> _ring_buffer_miss_msg;
    private ArtemisMissCheckerProcessor _miss_check_processor;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());
    private final Bytes<ByteBuffer> _byte_sub_msg = Bytes.elasticByteBuffer();          // dùng để xử lý msg lấy từ sub msg
    private final Bytes<ByteBuffer> _byte_process_msg = Bytes.elasticByteBuffer();     // dùng để xử lý msg trong process


    public Artemis(ArtemisCfg cfg, ArtemisHandler onData, Consumer<String> onInterrupt, Consumer<String> onWarning) {
        // validate
        Utils.checkNull(cfg.getSourceIP(), "Require source IP");
        Utils.checkNull(onData, "Require on data");
        Utils.checkNull(onInterrupt, "Require on interrupt");

        _cfg = cfg;
        _onData = onData;
        _onInterrupt = onInterrupt;
        _onWarning = onWarning;
        _object_pool = new ObjectPool<>(cfg.getMaxObjectsPoolWait(), ArtemisCacheMsg::new);
        _zmq_context = new ZContext();

        _status.set(STARTING);

        // khởi tạo luồng chính
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Artemis ALL",
                        true,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        this::_mainProcess));

        // sau bao lâu thì bắt đầu chọn seq và chạy chính thức
        new Thread(() -> {
            LockSupport.parkNanos(_cfg.getStartingTimeMs() * 1000000L);
            _ring_buffer_process_msg.publishEvent(
                    (newEvent, sequence, __type, __bytesParam) -> {
                        newEvent.setType(__type);
                        newEvent.setData(__bytesParam);
                    },
                    Constance.ARTEMIS.PROCESSS_MSG_TYPE.INIT,
                    _byte_miss_msg_raw);
        }).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }


    private void _mainProcess() {
        log.info("Artemis run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo disruptor xử lý các msg chính
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Artemis Disruptor Process Msg",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableDisruptorProcessMsgBindingCore(),
                        _cfg.getDisruptorProcessMsgCpu(),
                        this::_initDisruptorProcessMsg));

        // Control miss msg
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Artemis Check Miss Msg",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableCheckMissMsgBindingCore(),
                        _cfg.getCheckMissMsgCpu(),
                        this::_initCheckMissMsg));

        // Control subscribe msg
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Artemis Subscribe Msg",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableSubMsgBindingCore(),
                        _cfg.getSubMsgCpu(),
                        () -> new Thread(this::_subMsg).start()));
    }


    /**
     * xử lý các msg được gửi tới
     * sau đó check xem còn msg chờ trong cache ko thì lấy ra xử lý
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void _initDisruptorProcessMsg() {
        log.info("Artemis run disruptor process msg on logical processor {}", Affinity.getCpu());

        _disruptor_process_msg = new Disruptor<>(
                ArtemisProcessMsg::new,
                _cfg.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                _cfg.getWaitStrategy());
        _disruptor_process_msg.handleEventsWith((event, sequence, endOfBatch) -> this._onMsg(event));
        _disruptor_process_msg.start();
        _ring_buffer_process_msg = _disruptor_process_msg.getRingBuffer();
    }


    /**
     * Check các msg bị miss
     */
    private void _initCheckMissMsg() {
        log.info("Artemis run check miss msg on logical processor {}", Affinity.getCpu());

        // Check xem có msg nào bị miss ko
        // Có 2 chế độ là "lấy msg mới nhất" và "lấy msg nằm giữa 2 index"
        _disruptor_miss_msg = new Disruptor<>(
                CheckMissMsgStateless::new,
                2 << 7,    // 256
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        _disruptor_miss_msg.start();
        _ring_buffer_miss_msg = _disruptor_miss_msg.getRingBuffer();

        // lắng nghe các yêu cầu lấy msg bị miss
        _miss_check_processor = new ArtemisMissCheckerProcessor(
                _ring_buffer_miss_msg,
                _ring_buffer_miss_msg.newBarrier(),
                _zmq_context,
                _cfg.getConfirmUrl(),
                _cfg.getTimeoutSendReqMissMsg(),
                _cfg.getTimeoutRecvReqMissMsg(),
                this::_onMissMsgReq);
        new Thread(_miss_check_processor).start();

        // định kỳ check xem có msg mới ko
        // định kỳ check xem msg trong hàng đợi có chờ quá lâu ko
        _extor_check_msg.scheduleAtFixedRate(this::_checkLatestMsg, 1000, _cfg.getTimeRateGetLatestMsgMS(), TimeUnit.MILLISECONDS);
        _extor_check_msg.scheduleAtFixedRate(this::_checkLongTimeMsg, 10, _cfg.getTimeRateGetMissMsgMS(), TimeUnit.MILLISECONDS);
    }


    /**
     * định kỳ lấy bản ghi mới nhất từ Odin về
     * trong luồng chính xử lý msg, nếu bản ghi này đã tồn tại thì bỏ qua
     * nếu là bản ghi tiếp theo thì xử lý
     * nếu là bản ghi xa hơn nữa thì để vào trong hàng chờ
     * nếu lâu quá chưa được xử lý thì sẽ có "checkLongTimeMsg" định kỳ check để lấy ra xử lý
     */
    private void _checkLatestMsg() {
        if (_status.get() != STOP) {
            _ring_buffer_miss_msg.publishEvent(
                    (newEvent, sequence, __type) -> newEvent.setType(__type),
                    Constance.ODIN.CONFIRM.LATEST_MSG);
        }
    }


    /**
     * đẩy vào trong luồng xử lý msg chính để lấy ra các msg đã vào nhưng lâu được xử lý
     */
    private void _checkLongTimeMsg() {
        if (_status.get() == RUNNING) {
            _ring_buffer_process_msg.publishEvent(
                    (newEvent, sequence, __type) -> newEvent.setType(__type),
                    Constance.ARTEMIS.PROCESSS_MSG_TYPE.CHECK_MISS);
        }
    }


    // Sub các msg được Odin stream sang
    private void _subMsg() {
        log.info("Artemis run subscribe msg on logical processor {}", Affinity.getCpu());

        ZMQ.Socket subscriber = _zmq_context.createSocket(SocketType.SUB);
        subscriber.setRcvHWM(_cfg.getZmqSubBufferSize());   // setting buffer size các msg được nhận

        /*
         * setHeartbeatIvl: interval gửi heartbeat
         * setHeartbeatTtl: đoạn này có vẻ dùng cho server hơn (config này được gửi cho client, để client biết được sau bao lâu ko có msg bất kỳ thì client tự hiểu là connect này đã bị chết. Client có thể tạo 1 connect mới để kết nối lại)
         * setHeartbeatTimeout: kể từ lúc gửi msg ping, sau 1 thời gian nhất định mà ko có msg mới nào thì đánh dấu kết nối tới server đã chết
         * setReconnectIVL: interval time reconnect lại nếu connect tới server gặp lỗi
         * setReconnectIVLMax: trong zmq, sau mỗi lần reconnect ko thành công, nó sẽ x2 thời gian chờ lên và connect lại. Giá trị sau khi x2 cũng ko vượt quá "setReconnectIVLMax"
         */
        subscriber.setHeartbeatIvl(10000);
        subscriber.setHeartbeatTtl(15000);
        subscriber.setHeartbeatTimeout(15000);
        subscriber.setReconnectIVL(10000);
        subscriber.setReconnectIVLMax(10000);

        log.info("Realtime url {}", _cfg.getRealTimeUrl());

        subscriber.connect(_cfg.getRealTimeUrl());
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);     // nhận tất cả tin nhắn từ publisher

        try {
            // Nhận và xử lý tin nhắn
            while (_status.get() == RUNNING || _status.get() == STARTING) {
                byte[] data = subscriber.recv(0);
                _ring_buffer_process_msg.publishEvent(
                        (newEvent, sequence, __data) -> {
                            newEvent.setType(Constance.ARTEMIS.PROCESSS_MSG_TYPE.MSG);
                            newEvent.setData(__data);
                        },
                        data);
            }
        } catch (Exception ex) {
            log.error("Artemis SubMsg error", ex);
        } finally {
            subscriber.close();
            log.info("Artemis end subscribe");
        }
    }


    /**
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void _onMsg(ArtemisProcessMsg event) {
        try {
            _byte_sub_msg.clear();

            if (event.getType() == Constance.ARTEMIS.PROCESSS_MSG_TYPE.MSG) {
                // chỉ có 1 msg duy nhất

                _byte_sub_msg.write(event.getData());
                _processOneMsg(_byte_sub_msg);
                _checkWaitMsgCanProcess();
            } else if (event.getType() == Constance.ARTEMIS.PROCESSS_MSG_TYPE.MULTI_MSG) {
                // có thể có nhiều msg

                // đọc cho tới khi nào hết data thì thôi
                _byte_sub_msg.write(event.getData());
                _processMultiMsg(_byte_sub_msg);
                _checkWaitMsgCanProcess();
            } else if (event.getType() == Constance.ARTEMIS.PROCESSS_MSG_TYPE.CHECK_MISS) {
                // nếu hàng chờ còn msg và nó đã chờ quá lâu --> lấy các msg miss ở giữa
                // nếu đã chờ rất rất lâu thì interrupt --> báo cho tầng ứng dụng biết
                // nếu ko có msg chờ thì bỏ qua
                if (!_msg_wait.isEmpty() && _status.get() == RUNNING) {
                    ArtemisCacheMsg msg = _msg_wait.firstEntry().getValue();
                    if (msg.getRcvTime() + _cfg.getTimeoutMustResyncMs() < System.currentTimeMillis()) {
                        _status.set(STOP);
                        _onInterrupt.accept("Msg wait so long");
                    } else if (msg.getRcvTime() + _cfg.getMaxTimeWaitMS() < System.currentTimeMillis()) {
                        long from = _seq + 1;
                        long to = msg.getSequence() - 1;
                        long currentIndex = from;
                        while (currentIndex <= to) {
                            _ring_buffer_miss_msg.publishEvent(
                                    (newEvent, sequence, __type, __seqFrom, __seqTo) -> {
                                        newEvent.setType(__type);
                                        newEvent.setSeqFrom(__seqFrom);
                                        newEvent.setSeqTo(__seqTo);
                                    },
                                    Constance.ODIN.CONFIRM.FROM_TO,
                                    currentIndex,
                                    Math.min(currentIndex + _cfg.getBatchSizeFromTo() - 1, to));
                            currentIndex += _cfg.getBatchSizeFromTo();
                        }
                    }
                }
            } else if (event.getType() == Constance.ARTEMIS.PROCESSS_MSG_TYPE.INIT) {
                // chuyển đổi trạng thái thành RUNNING
                _status.set(RUNNING);
                // khi bắt đầu subscribe msg từ Odin thì seq thường ko bằng "0". Do đó ta cần lấy seq có giá trị nhỏ nhất
                if (_seq == 0 && !_msg_wait.isEmpty()) {
                    _seq = _msg_wait.firstKey() - 1;
                    _checkWaitMsgCanProcess();
                }
            }
        } catch (Exception ex) {
            log.error("Artemis OnMsg error, msg {}", event.toString(), ex);
        }
    }


    // kiểm tra xem có msg chờ nào có thể xử lý không
    private void _checkWaitMsgCanProcess() {
        while (!_msg_wait.isEmpty() && _msg_wait.firstKey() <= _seq + 1) {
            ArtemisCacheMsg msg = _msg_wait.remove(_msg_wait.firstKey());
            _processOneMsg(msg);
        }
    }


    private void _processMultiMsg(Bytes<ByteBuffer> bytesIn) {
        while (bytesIn.readRemaining() > 0) {
            _processOneMsg(bytesIn);
        }
    }


    // "bytesIn" dữ liệu được bên source gửi về. Có thể chứa 1 hoặc nhiều bản ghi liên tiếp
    private void _processOneMsg(Bytes<ByteBuffer> bytesIn) {
        try {
            ArtemisCacheMsg msg = _object_pool.pop();
            msg.setVersion(bytesIn.readLong());
            msg.setSequence(bytesIn.readLong());

            _byte_process_msg.clear();
            bytesIn.read(_byte_process_msg, bytesIn.readInt());
            msg.getData().clear();
            msg.getData().write(_byte_process_msg);

            _processOneMsg(msg);
        } catch (Exception ex) {
            log.error("Artemis ProcessOneMsg error, bytes {}", Arrays.toString(bytesIn.toByteArray()), ex);
        }
    }


    private void _processOneMsg(ArtemisCacheMsg msg) {
        try {
            // version mới --> ko xử lý, reset lại toàn bộ waitlist và thông báo cho ứng dụng bên ngoài biết cần restart lại
            if (msg.getVersion() != _version && _version > 0) {
                if (_status.compareAndSet(RUNNING, STOP)) {
                    while (!_msg_wait.isEmpty()) {
                        ArtemisCacheMsg cacheMsg = _msg_wait.remove(_msg_wait.firstKey());
                        _object_pool.push(cacheMsg);
                    }
                    _status.set(STOP);
                    _onInterrupt.accept("Change version");
                }
                _object_pool.push(msg);
                return;
            }

            _version = msg.getVersion();

            // (msg đã xử lý || version cũ) --> thì bỏ qua
            if (msg.getSequence() <= _seq) {
                _object_pool.push(msg);
                return;
            }

            if (msg.getSequence() == _seq + 1) {
                // nếu là msg nối tiếp thì xử lý luôn
                _seq++;
                _onData.apply(msg.getVersion(), msg.getSequence(), msg.getData());
                _object_pool.push(msg);
            } else {
                // msg lớn hơn "current + 1" thì đẩy vào trong hệ thống chờ kèm thời gian
                msg.setRcvTime(System.currentTimeMillis());
                _msg_wait.put(msg.getSequence(), msg);
            }
        } catch (Exception ex) {
            log.error("Artemis ProcessOneMsg error, msg {}", msg, ex);
        }
    }


    /**
     * lấy msg mới nhất của src
     * xử lý các yêu cầu lấy msg bị miss từ [index_from, index_to]
     * trả về "true" nếu việc gửi/nhận thành công và ngược lại
     */
    private boolean _onMissMsgReq(ZMQ.Socket _zSocket, CheckMissMsgStateless msg) {
        try {
            boolean isSuccess = true;

            if (msg.getType() == Constance.ODIN.CONFIRM.LATEST_MSG) {
                // nếu lấy msg cuối cùng

                // gửi đi
                _byte_miss_msg.writeByte(msg.getType());
                isSuccess = _zSocket.send(_byte_miss_msg.toByteArray());
                _byte_miss_msg.clear();

                if (!isSuccess) {
                    // ko gửi được msg

                    log.error("Get latest msg fail. Maybe TIMEOUT !");
                    if (_onWarning != null) _onWarning.accept("Req msg maybe TIMEOUT !");
                } else {
                    // nhận về
                    _byte_miss_msg_raw = _zSocket.recv();
                    if (_byte_miss_msg_raw == null) {
                        log.error("Rep latest msg empty. Maybe TIMEOUT !");
                        if (_onWarning != null) _onWarning.accept("Req msg maybe TIMEOUT !");
                        isSuccess = false;
                    } else if (_byte_miss_msg_raw.length > 0) {
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __type, __bytesParam) -> {
                                    newEvent.setType(__type);
                                    newEvent.setData(__bytesParam);
                                },
                                Constance.ARTEMIS.PROCESSS_MSG_TYPE.MSG,
                                _byte_miss_msg_raw);
                    }
                }
            } else if (msg.getType() == Constance.ODIN.CONFIRM.FROM_TO) {
                // nếu lấy các bản ghi from-to index

                _byte_miss_msg.writeByte(msg.getType());
                _byte_miss_msg.writeLong(msg.getSeqFrom());
                _byte_miss_msg.writeLong(msg.getSeqTo());

                // gửi đi
                isSuccess = _zSocket.send(_byte_miss_msg.toByteArray());
                _byte_miss_msg.clear();

                if (!isSuccess) {
                    // ko gửi được msg

                    log.error(MessageFormat.format("Get items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getSeqFrom(), msg.getSeqTo()));
                    if (_onWarning != null) _onWarning.accept("Req msg maybe TIMEOUT !");
                } else {
                    // nhận về
                    _byte_miss_msg_raw = _zSocket.recv();
                    if (_byte_miss_msg_raw == null) {
                        log.error(MessageFormat.format("Rep items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getSeqFrom(), msg.getSeqTo()));
                        if (_onWarning != null) _onWarning.accept("Req msg maybe TIMEOUT !");
                        isSuccess = false;
                    } else if (_byte_miss_msg_raw.length > 0) {
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __type, __bytesParam) -> {
                                    newEvent.setType(__type);
                                    newEvent.setData(__bytesParam);
                                },
                                Constance.ARTEMIS.PROCESSS_MSG_TYPE.MULTI_MSG,
                                _byte_miss_msg_raw);
                    }
                }
            }

            return isSuccess;
        } catch (Exception ex) {
            log.error("Artemis OnMissMsgReq error, msg {}", msg.toString(), ex);
            return false;
        }
    }


    /**
     * Lần lượt shutdown và giải phóng tài nguyên theo thứ tự từ đầu vào --> đầu ra
     * các công việc đang làm dở sẽ làm nốt cho xong
     */
    public void shutdown() {
        log.info("Artemis closing...");

        _status.set(STOP);
        LockSupport.parkNanos(500_000_000);

        // close zeromq, ngừng nhận msg mới
        _zmq_context.destroy();

        // turnoff miss check processor, ngừng việc lấy các msg thiếu và msg mới nhất
        _extor_check_msg.shutdownNow();
        _miss_check_processor.halt();
        _disruptor_miss_msg.shutdown();

        // close disruptor, ngừng nhận msg mới, xử lý nốt msg trong ring_buffer
        _disruptor_process_msg.shutdown();
        // ngừng 2s để xử lý nốt msg trong ring buffer
        LockSupport.parkNanos(1_000_000_000);

        // object pool
        _object_pool.clear();

        // byte
        _byte_miss_msg.releaseLast();
        _byte_sub_msg.releaseLast();
        _byte_process_msg.releaseLast();

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(500_000_000);

        log.info("Artemis CLOSED !");
    }


}
