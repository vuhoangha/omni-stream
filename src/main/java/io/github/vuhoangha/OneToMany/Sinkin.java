package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.*;
import io.github.vuhoangha.common.SynchronizeObjectPool;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class Sinkin {

    private final int IDLE = 0, SYNCING = 1, RUNNING = 2, STOP = 3;
    private final AtomicInteger _status = new AtomicInteger(IDLE);

    ScheduledExecutorService _execs_check_msg = Executors.newScheduledThreadPool(1);
    private final NavigableMap<Long, TranspotMsg> _msg_wait = new TreeMap<>();
    private final SynchronizeObjectPool<TranspotMsg> _transpot_msg_pool;
    private final SinkinCfg _cfg;
    private final SingleChronicleQueue _queue;
    private final ExcerptAppender _appender;
    private long _seq_in_queue;
    private long _src_latest_index;                                                 // src index của item mới nhất trong queue
    private final ZContext _zmq_context;
    private final Bytes<ByteBuffer> _byte_miss_msg = Bytes.elasticByteBuffer();     // dùng để tạo data lấy msg miss
    private final Bytes<ByteBuffer> _byte_process_msg = Bytes.elasticByteBuffer();     // dùng để tạo data xử lý msg
    private Disruptor<SinkProcessMsg> _disruptor_process_msg;
    private RingBuffer<SinkProcessMsg> _ring_buffer_process_msg;
    private Disruptor<CheckMissMsg> _disruptor_miss_msg;
    private RingBuffer<CheckMissMsg> _ring_buffer_miss_msg;
    private SinkinMissCheckerProcessor _miss_check_processor;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());
    private final SinkinHandler _handler;


    public Sinkin(SinkinCfg cfg, SinkinHandler handler) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");
        Utils.checkNull(cfg.getSourceIP(), "Require source IP");
        Utils.checkNull(handler, "Require handler");

        _cfg = cfg;
        _handler = handler;

        _transpot_msg_pool = new SynchronizeObjectPool<>(new TranspotMsg[cfg.getMaxObjectsPoolWait()], TranspotMsg::new);
        _zmq_context = new ZContext();

        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(_cfg.getRollCycles())
                .build();
        _appender = _queue.acquireAppender();
        LongLongObject lastestInfo = _getLatestInfo();
        if (lastestInfo != null) {
            _src_latest_index = lastestInfo.valueA;
            if (lastestInfo.valueB != _queue.entryCount()) {
                throw new RuntimeException(MessageFormat.format("Sequence in queue not match, latestInfo {0}, entryCount {1}", lastestInfo.valueB, _queue.entryCount()));
            } else {
                _seq_in_queue = _queue.entryCount();   // tổng số item trong queue
            }
        } else {
            _src_latest_index = -1;
            _seq_in_queue = 0;
        }

        _status.set(SYNCING);

        // bắt đầu lắng nghe việc ghi vào queue
        new Thread(this::_onWriteQueue).start();
        LockSupport.parkNanos(1_000_000_000L);

        // chạy đồng bộ dữ liệu với source trước
        new Thread(this::_sync).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    /**
     * Hàm này sẽ đồng bộ hoàn toàn msg từ src --> sink. Khi đã hoàn thành thì mới sub msg mới
     * cấu trúc request: ["type"]["src index"]
     * Cấu trúc response: ["msg_1"]["msg_2"]...vvv
     * cấu trúc từng msg con msg_1, msg_2..vv:  ["source native index 1"]["độ dài data 1 gồm cả seq in queue"]["seq in queue 1"]["data 1"]
     */
    private void _sync() {
        ZMQ.Socket socket = _zmq_context.createSocket(SocketType.REQ);
        socket.connect(_cfg.getConfirmUrl());

        ExcerptAppender localAppender = _queue.acquireAppender();
        Bytes<ByteBuffer> byteRes = Bytes.elasticByteBuffer();   // chứa byte[] của tất cả bản ghi trả về
        Bytes<ByteBuffer> byteToQueue = Bytes.elasticByteBuffer();   // chứa byte[] để ghi vào queue

        try {
            while (_status.get() == SYNCING) {
                // tổng hợp data rồi req sang src
                _byte_miss_msg.writeByte(Constance.FANOUT.CONFIRM.FROM_LATEST);
                _byte_miss_msg.writeLong(_src_latest_index);
                socket.send(_byte_miss_msg.underlyingObject().array(), 0);
                _byte_miss_msg.clear();

                // chờ dữ liệu trả về
                byte[] repData = socket.recv(0);

                // nếu đã hết msg thì thôi
                if (repData.length == 0) {
                    log.info("Sinkin synced");
                    break;
                }

                log.info("Sinkin syncing......");

                // đọc tuần tự và xử lý
                byteRes.write(repData);

                while (byteRes.readRemaining() > 0) {

                    // lấy phần dữ liệu để ghi vào queue
                    int dataLen = byteRes.readInt();
                    byteToQueue.write(byteRes, byteRes.readPosition(), 8 + 8 + dataLen);

                    long srcIndex = byteRes.readLong();
                    long seq = byteRes.readLong();
                    byteRes.readSkip(dataLen);              // bỏ qua data chính

                    // nếu msg không tuần tự thì báo lỗi luôn
                    if (seq != _seq_in_queue + 1) {
                        log.error("Sinkin _sync not sequence, src_seq: {}, sink_seq: {}", seq, _seq_in_queue);
                        return;
                    }

                    // update seq và native index
                    _seq_in_queue++;
                    _src_latest_index = srcIndex;

                    // write to queue
                    localAppender.writeBytes(byteToQueue);

                    // clear temp data
                    byteToQueue.clear();
                }
                byteRes.clear();
            }

            _status.set(RUNNING);

            // khởi tạo luồng chính
            _affinity_composes.add(
                    Utils.runWithThreadAffinity(
                            "Sinkin ALL",
                            true,
                            _cfg.getEnableBindingCore(),
                            _cfg.getCpu(),
                            _cfg.getEnableBindingCore(),
                            _cfg.getCpu(),
                            this::_mainProcess));
        } catch (Exception ex) {
            log.error("Sinkin sync error", ex);
        } finally {
            // giải phóng tài nguyên
            localAppender.close();
            byteRes.releaseLast();
            byteToQueue.releaseLast();
            socket.close();

            log.info("Sinkin synced done !");
        }
    }


    private void _mainProcess() {
        log.info("Sinkin run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo disruptor xử lý các msg chính
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Disruptor Process Msg",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableDisruptorProcessMsgBindingCore(),
                        _cfg.getDisruptorProcessMsgCpu(),
                        this::_initDisruptorProcessMsg));

        // Control miss msg and subscribe queue
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Check Miss Msg And Sub Queue",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableCheckMissMsgAndSubQueueBindingCore(),
                        _cfg.getCheckMissMsgAndSubQueueCpu(),
                        this::_initCheckMissMsg));

        // Control miss msg and subscribe queue
        _affinity_composes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Subscribe Msg",
                        false,
                        _cfg.getEnableBindingCore(),
                        _cfg.getCpu(),
                        _cfg.getEnableSubMsgBindingCore(),
                        _cfg.getSubMsgCpu(),
                        () -> new Thread(this::_subMsg).start()));
    }


    /**
     * xử lý các msg được gửi tới và lưu vào queue
     * sau đó check xem còn msg chờ trong cache ko thì lấy ra xử lý
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void _initDisruptorProcessMsg() {
        log.info("Sinkin run disruptor process msg on logical processor {}", Affinity.getCpu());

        _disruptor_process_msg = new Disruptor<>(
                SinkProcessMsg::new,
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
        log.info("Sinkin run check miss msg and subscribe queue on logical processor {}", Affinity.getCpu());

        /*
         * Check xem có msg nào bị miss ko
         * Có 2 chế độ là "lấy msg mới nhất" và "lấy msg nằm giữa 2 index"
         */
        _disruptor_miss_msg = new Disruptor<>(
                CheckMissMsg::new,
                2 << 7,    // 256
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        _disruptor_miss_msg.start();
        _ring_buffer_miss_msg = _disruptor_miss_msg.getRingBuffer();

        // lắng nghe các yêu cầu lấy msg bị miss
        _miss_check_processor = new SinkinMissCheckerProcessor(
                _ring_buffer_miss_msg,
                _ring_buffer_miss_msg.newBarrier(),
                _zmq_context,
                _cfg.getConfirmUrl(),
                _cfg.getTimeoutSendReqMissMsg(),
                _cfg.getTimeoutRecvReqMissMsg(),
                this::_onMissMsgReq);
        new Thread(_miss_check_processor).start();

        /*
         * định kỳ check xem có msg mới ko
         * định kỳ check xem msg trong hàng đợi có chờ quá lâu ko
         */
        _execs_check_msg.scheduleAtFixedRate(this::_checkLatestMsg, 1000, _cfg.getTimeRateGetLatestMsgMS(), TimeUnit.MILLISECONDS);
        _execs_check_msg.scheduleAtFixedRate(this::_checkLongTimeMsg, 10, _cfg.getTimeRateGetMissMsgMS(), TimeUnit.MILLISECONDS);
    }


    /**
     * định kỳ lấy bản ghi mới nhất từ queue về
     * trong luồng chính xử lý msg, nếu bản ghi này đã tồn tại thì bỏ qua
     * nếu là bản ghi tiếp theo thì xử lý
     * nếu là bản ghi xa hơn nữa thì để vào trong hàng chờ
     * nếu lâu quá chưa được xử lý thì sẽ có "checkLongTimeMsg" định kỳ check để lấy ra xử lý
     */
    private void _checkLatestMsg() {
        _ring_buffer_miss_msg.publishEvent(
                (newEvent, sequence, __type) -> newEvent.setType(__type),
                Constance.FANOUT.CONFIRM.LATEST_MSG);
    }


    /**
     * đẩy vào trong luồng xử lý msg chính để lấy ra các msg đã vào nhưng lâu được xử lý
     */
    private void _checkLongTimeMsg() {
        _ring_buffer_process_msg.publishEvent(
                (newEvent, sequence, __type) -> newEvent.setType(__type),
                Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS);
    }


    /**
     * Sub các msg được src stream sang
     */
    private void _subMsg() {
        log.info("Sinkin run subscribe msg on logical processor {}", Affinity.getCpu());

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

        subscriber.connect(_cfg.getRealTimeUrl());
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);     // nhận tất cả tin nhắn từ publisher

        log.info("Sinkin start subscribe");

        // Nhận và xử lý tin nhắn
        while (_status.get() == RUNNING) {
            try {
                byte[] msg = subscriber.recv(0);
                _ring_buffer_process_msg.publishEvent(
                        (newEvent, sequence, __bytesData) -> newEvent.setType(__bytesData[0]).setData(__bytesData),     // vì byte đầu tiên luôn là type nên ta lấy luôn
                        msg);
            } catch (Exception ex) {
                log.error("Sinkin SubMsg error", ex);
            }
        }

        subscriber.close();
        log.info("Sinkin end subscribe");
    }


    /**
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void _onMsg(SinkProcessMsg event) {
        try {
            if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MSG) {
                // chỉ có 1 msg duy nhất. Cấu trúc ["msg type"]["source native index 1"]["seq in queue 1"]["data 1"]

                _byte_process_msg.write(event.getData());

                TranspotMsg tMsg = _transpot_msg_pool.pop();

                tMsg.getQueueData().write(_byte_process_msg, 1, _byte_process_msg.readLimit() - 1);

                _byte_process_msg.readByte();       // byte đầu ko cần nên bỏ
                tMsg.setSrcIndex(_byte_process_msg.readLong());
                tMsg.setSeq(_byte_process_msg.readLong());
                tMsg.getData().write(_byte_process_msg, _byte_process_msg.readPosition(), _byte_process_msg.readRemaining());

                _processOneMsg(tMsg);

                _byte_process_msg.clear();
            } else if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MULTI_MSG) {
                // có thể có nhiều msg

                _byte_process_msg.write(event.getData());

                // đọc cho tới khi nào hết data thì thôi
                // Cấu trúc: ["độ dài data 1"]["source native index 1"]["seq in queue 1"]["data 1"]["độ dài data 2"]["source native index 2"]["seq in queue 2"]["data 2"]
                while (_byte_process_msg.readRemaining() > 0) {
                    TranspotMsg tMsg = _transpot_msg_pool.pop();

                    int dataLen = _byte_process_msg.readInt();
                    tMsg.getQueueData().write(_byte_process_msg, _byte_process_msg.readPosition(), 8 + 8 + dataLen);    // đọc nhưng ko ảnh hưởng readPosition trong _byte_process_msg
                    tMsg.setSrcIndex(_byte_process_msg.readLong());
                    tMsg.setSeq(_byte_process_msg.readLong());
                    _byte_process_msg.read(tMsg.getData(), dataLen);

                    _processOneMsg(tMsg);
                }

                _byte_process_msg.clear();
            } else if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS) {
                // nếu hàng chờ còn msg và nó đã chờ quá lâu --> lấy các msg miss ở giữa
                // nếu ko có msg chờ thì bỏ qua
                if (!_msg_wait.isEmpty()) {
                    TranspotMsg tMsg = _msg_wait.firstEntry().getValue();
                    if (tMsg.getRcvTime() + _cfg.getMaxTimeWaitMS() < System.currentTimeMillis()) {
                        _ring_buffer_miss_msg.publishEvent(
                                (newEvent, sequence, __type, __indexFrom, __indexTo) -> {
                                    newEvent.setType(__type);
                                    newEvent.setIndexFrom(__indexFrom);
                                    newEvent.setIndexTo(__indexTo);
                                },
                                Constance.FANOUT.CONFIRM.FROM_TO,
                                _src_latest_index,
                                tMsg.getSrcIndex());
                    }
                }
            }

            // check xem trong hàng đợi có msg kế tiếp không ?
            if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MSG || event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MULTI_MSG) {
                while (!_msg_wait.isEmpty() && _msg_wait.firstKey() <= _seq_in_queue + 1) {
                    long firstKey = _msg_wait.firstKey();
                    TranspotMsg tMsg = _msg_wait.remove(firstKey);      // lấy msg chờ và xóa khỏi hàng chờ
                    if (firstKey == _seq_in_queue + 1) {
                        _seq_in_queue++;
                        _src_latest_index = tMsg.getSrcIndex();
                        _appender.writeBytes(tMsg.getQueueData());                           // ghi vào queue
                    }

                    // clear data và trả về pool
                    tMsg.clear();
                    _transpot_msg_pool.push(tMsg);
                }
            }
        } catch (Exception ex) {
            log.error("Sinkin OnMsg error", ex);
        }
    }


    // xử lý 1 msg hoàn chỉnh
    private void _processOneMsg(TranspotMsg tMsg) {
        try {
            if (tMsg.getSeq() <= _seq_in_queue) {
                // msg đã xử lý thì bỏ qua
                // trả về pool
                tMsg.clear();
                _transpot_msg_pool.push(tMsg);
            } else if (tMsg.getSeq() == _seq_in_queue + 1) {
                // nếu là msg tiếp theo --> ghi vào trong queue
                _seq_in_queue++;
                _src_latest_index = tMsg.getSrcIndex();
                _appender.writeBytes(tMsg.getQueueData());
                // trả về pool
                tMsg.clear();
                _transpot_msg_pool.push(tMsg);
            } else {
                // msg lớn hơn "current + 1" thì đẩy vào trong hệ thống chờ kèm thời gian
                tMsg.setRcvTime(System.currentTimeMillis());
                _msg_wait.put(tMsg.getSeq(), tMsg);
            }
        } catch (Exception ex) {
            // trả về pool
            tMsg.clear();
            _transpot_msg_pool.push(tMsg);
            log.error("Sinkin ProcessOneMsg error", ex);
        }
    }


    /**
     * lấy msg mới nhất của src
     * xử lý các yêu cầu lấy msg bị miss từ [index_from, index_to]
     * trả về "true" nếu việc gửi/nhận thành công và ngược lại
     */
    private boolean _onMissMsgReq(ZMQ.Socket _zSocket, CheckMissMsg msg) {
        try {
            boolean isSuccess = true;

            if (msg.getType() == Constance.FANOUT.CONFIRM.LATEST_MSG) {
                // nếu lấy msg cuối cùng

                _byte_miss_msg.writeByte(Constance.FANOUT.CONFIRM.LATEST_MSG);


                // gửi đi
                isSuccess = _zSocket.send(_byte_miss_msg.toByteArray());
                _byte_miss_msg.clear();

                if (!isSuccess) {
                    // ko gửi được msg

                    log.error("Get latest msg fail. Maybe TIMEOUT !");
                } else {
                    // gửi thành công và nhận về
                    byte[] resData = _zSocket.recv();

                    if (resData == null) {
                        log.error("Rep latest msg empty. Maybe TIMEOUT !");
                        isSuccess = false;
                    } else if (resData.length > 0) {
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __bytesData) -> newEvent.setType(__bytesData[0]).setData(__bytesData),
                                resData);
                    }
                }
            } else if (msg.getType() == Constance.FANOUT.CONFIRM.FROM_TO) {
                // nếu lấy các bản ghi from-to index

                _byte_miss_msg.writeByte(Constance.FANOUT.CONFIRM.FROM_TO);
                _byte_miss_msg.writeLong(msg.getIndexFrom());
                _byte_miss_msg.writeLong(msg.getIndexTo());

                // gửi đi
                isSuccess = _zSocket.send(_byte_miss_msg.toByteArray());
                _byte_miss_msg.clear();

                if (!isSuccess) {
                    // ko gửi được msg
                    log.error(MessageFormat.format("Get items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getIndexFrom(), msg.getIndexTo()));
                } else {
                    // nhận về
                    byte[] resData = _zSocket.recv();
                    if (resData == null) {
                        log.error(MessageFormat.format("Rep items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getIndexFrom(), msg.getIndexTo()));
                        isSuccess = false;
                    } else if (resData.length > 0) {
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __type, __bytesData) -> newEvent.setType(__type).setData(__bytesData),
                                Constance.SINKIN.PROCESS_MSG_TYPE.MULTI_MSG, resData);
                    }
                }
            }

            return isSuccess;
        } catch (Exception ex) {
            log.error("Sinkin OnMissMsgReq error, msg {}", msg.toString(), ex);
            return false;
        }
    }


    /**
     * lấy index của item cuối cùng trong queue
     * mục đích là để xác định vị trí cuối cùng đã đồng bộ từ Source từ đó tiếp tục đồng bộ
     */
    private LongLongObject _getLatestInfo() {
        try {
            if (_queue.lastIndex() >= 0) {
                Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

                ExcerptTailer tailer = _queue.createTailer();
                tailer.moveToIndex(_queue.lastIndex());
                tailer.readBytes(bytes);

                long index = bytes.readLong();
                long seq = bytes.readLong();

                bytes.releaseLast();

                return new LongLongObject(index, seq);
            } else {
                return null;
            }
        } catch (Exception ex) {
            log.error("Sinkin GetLatestIndex error", ex);
            return null;
        }
    }


    /**
     * lắng nghe các event được viết vào queue và call cho "Handler"
     */
    private void _onWriteQueue() {
        ExcerptTailer tailer = _queue.createTailer();

        // di chuyển tới cuối
        tailer.toEnd();

        Bytes<ByteBuffer> bytesRead = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();

        Runnable waiter = OmniWaitStrategy.getWaiter(_cfg.getQueueWaitStrategy());

        while (_status.get() == RUNNING || _status.get() == SYNCING) {
            try {
                if (tailer.readBytes(bytesRead)) {
                    bytesRead.readSkip(8);      // bỏ qua src index
                    long seq = bytesRead.readLong();
                    bytesRead.read(bytesData, (int) bytesRead.readRemaining()); // data

                    _handler.apply(tailer.lastReadIndex(), seq, bytesData);

                    bytesRead.clear();
                    bytesData.clear();
                } else {
                    waiter.run();
                }
            } catch (Exception ex) {
                log.error("Sinkin OnWriteQueue error", ex);
            }
        }

        bytesRead.releaseLast();
        bytesData.releaseLast();
        tailer.close();

        log.info("Sinkin subscribe write queue closed");
    }


    /**
     * Lần lượt shutdown và giải phóng tài nguyên theo thứ tự từ đầu vào --> đầu ra
     * các công việc đang làm dở sẽ làm nốt cho xong
     */
    private void _onShutdown() {
        log.info("Sinkin closing...");

        _status.set(STOP);
        LockSupport.parkNanos(500_000_000);

        // close zeromq, ngừng nhận msg mới
        _zmq_context.destroy();

        // turnoff miss check processor, ngừng việc lấy các msg thiếu và msg mới nhất
        _execs_check_msg.shutdownNow();
        _miss_check_processor.halt();
        _disruptor_miss_msg.shutdown();

        // close disruptor, ngừng nhận msg mới, xử lý nốt msg trong ring_buffer
        _disruptor_process_msg.shutdown();
        // ngừng 2s để xử lý nốt msg trong ring buffer
        LockSupport.parkNanos(1_000_000_000);

        // close chronicle queue
        _appender.close();
        _queue.close();

        // object pool
        _transpot_msg_pool.clear();

        // byte
        _byte_miss_msg.releaseLast();

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(500_000_000);

        log.info("Sinkin CLOSED !");
    }


}
