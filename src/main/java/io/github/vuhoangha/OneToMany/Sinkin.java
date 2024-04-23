package io.github.vuhoangha.OneToMany;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.vuhoangha.Common.*;
import net.openhft.affinity.Affinity;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.core.io.ReferenceOwner;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class Sinkin<T extends SelfDescribingMarshallable> {

    private static final int IDLE = 0, SYNCING = 1, RUNNING = 2, STOP = 3;
    private int _status = IDLE;

    private static final Logger LOGGER = LoggerFactory.getLogger(Sinkin.class);
    private final ReferenceOwner _ref_id = ReferenceOwner.temporary("Sinkin");
    ScheduledExecutorService _extor_check_msg = Executors.newScheduledThreadPool(1);
    private final NavigableMap<Long, TranspotMsg> _msg_wait = new TreeMap<>();
    private final ObjectPool<TranspotMsg> _object_pool;
    private final SinkinHandler _handler;
    private final Class<T> _dataType;
    private final SinkinCfg _cfg;
    private final SingleChronicleQueue _queue;
    private final ExcerptAppender _appender;
    private long _seq_in_queue;
    private long _src_latest_index;                                                 // src index của item mới nhất trong queue
    private final ZContext _zmq_context;
    private final Bytes<ByteBuffer> _byte_miss_msg = Bytes.elasticByteBuffer();     // dùng để tạo data lấy msg miss
    private byte[] _byte_miss_msg_raw;                                              // dùng để lấy msg miss raw
    private Disruptor<SinkProcessMsg> _disruptor_process_msg;
    private RingBuffer<SinkProcessMsg> _ring_buffer_process_msg;
    private Disruptor<CheckMissMsg> _disruptor_miss_msg;
    private RingBuffer<CheckMissMsg> _ring_buffer_miss_msg;
    private SinkinMissCheckerProcessor _miss_check_processor;
    List<AffinityCompose> _affinity_composes = Collections.synchronizedList(new ArrayList<>());


    public Sinkin(SinkinCfg cfg, Class<T> dataType, SinkinHandler handler) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");
        Utils.checkNull(cfg.getSourceIP(), "Require source IP");
        Utils.checkNull(dataType, "Require dataType");
        Utils.checkNull(handler, "Require handler");

        _cfg = cfg;
        _dataType = dataType;
        _handler = handler;
        _object_pool = new ObjectPool<>(cfg.getMaxObjectsPoolWait(), TranspotMsg.class);
        _zmq_context = new ZContext();

        _queue = SingleChronicleQueueBuilder
                .binary(_cfg.getQueuePath())
                .rollCycle(_cfg.getRollCycles())
                .build();
        _appender = _queue.acquireAppender();
        _seq_in_queue = _queue.entryCount();   // tổng số item trong queue
        _src_latest_index = _getLatestIndex();

        // tạm dừng 100ms để _status được thấy bởi tất cả các Thread
        _status = SYNCING;
        LockSupport.parkNanos(100_000_000);

        // chạy đồng bộ dữ liệu với source trước
        new Thread(this::_sync).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    /**
     * Hàm này sẽ đồng bộ hoàn toàn msg từ src --> sink. Khi đã hoàn thành thì mới sub msg mới
     * cấu trúc request: ["type"]["src index"]
     * Cấu trúc response: ["msg_1"]["msg_2"]...vvv
     * cấu trúc từng msg con msg_1, msg_2..vv:  ["version"]["độ dài data"]["data"]["seq in queue"]["src index"]
     */
    private void _sync() {
        ZMQ.Socket socket = _zmq_context.createSocket(SocketType.REQ);
        socket.connect(_cfg.getConfirmUrl());

        ExcerptAppender localAppender = _queue.acquireAppender();
        Bytes<ByteBuffer> byteRes = Bytes.elasticByteBuffer();   // chứa byte[] của tất cả bản ghi trả về
        TranspotMsg transpotMsg = new TranspotMsg();

        try {
            while (_status == SYNCING) {
                // tổng hợp data rồi req sang src
                _byte_miss_msg.writeByte(Constance.FANOUT.CONFIRM.FROM_LATEST);
                _byte_miss_msg.writeLong(_src_latest_index);
                socket.send(_byte_miss_msg.toByteArray(), 0);
                _byte_miss_msg.clear();

                // chờ dữ liệu trả về
                byte[] repData = socket.recv(0);

                // nếu đã hết msg thì thôi
                if (repData.length == 0) {
                    LOGGER.info("Sinkin synced");
                    break;
                }

                LOGGER.info("Sinkin syncing......");

                // đọc tuần tự và xử lý
                byteRes.write(repData);
                while (byteRes.readRemaining() > 0) {

                    // đọc từng field trong 1 item
                    transpotMsg.setVersion(byteRes.readByte());
                    byteRes.read(transpotMsg.getData(), byteRes.readInt());
                    transpotMsg.setSeq(byteRes.readLong());
                    transpotMsg.setSrcIndex(byteRes.readLong());

                    // nếu msg không tuần tự thì báo lỗi luôn
                    if (transpotMsg.getSeq() != _seq_in_queue + 1) {
                        LOGGER.error("Sinkin _sync not sequence, src_seq: {}, sink_seq: {}", transpotMsg.getSeq(), _seq_in_queue);
                        return;
                    }

                    // update seq và native index
                    _seq_in_queue++;
                    _src_latest_index = transpotMsg.getSrcIndex();

                    // write to queue
                    localAppender.writeBytes(transpotMsg.toBytes());

                    // clear temp data
                    transpotMsg.clear();
                }
                byteRes.clear();
            }

            // tạm dừng 100ms để _status được thấy bởi tất cả các Thread
            _status = RUNNING;
            LockSupport.parkNanos(100_000_000);

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
            LOGGER.error("Sinkin sync error", ex);
        } finally {
            // giải phóng tài nguyên
            localAppender.close();
            byteRes.release(_ref_id);
            transpotMsg.destroy(_ref_id);
            socket.close();

            LOGGER.info("Sinkin synced done !");
        }
    }


    private void _mainProcess() {
        LOGGER.info("Sinkin run main flow on logical processor {}", Affinity.getCpu());

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
                        this::_initCheckMissMsgAndListenQueue));

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
        LOGGER.info("Sinkin run disruptor process msg on logical processor {}", Affinity.getCpu());

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
     * Check các msg bị miss và lắng nghe các msg mới được ghi vào queue để gửi cho application
     */
    private void _initCheckMissMsgAndListenQueue() {
        LOGGER.info("Sinkin run check miss msg and subscribe queue on logical processor {}", Affinity.getCpu());

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
        _extor_check_msg.scheduleAtFixedRate(this::_checkLatestMsg, 1000, _cfg.getTimeRateGetLatestMsgMS(), TimeUnit.MILLISECONDS);
        _extor_check_msg.scheduleAtFixedRate(this::_checkLongTimeMsg, 10, _cfg.getTimeRateGetMissMsgMS(), TimeUnit.MILLISECONDS);

        // bắt đầu lắng nghe việc ghi vào queue
        new Thread(this::_onWriteQueue).start();
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
                Constance.SINKIN.PROCESSS_MSG_TYPE.CHECK_MISS);
    }


    /**
     * Sub các msg được src stream sang
     */
    private void _subMsg() {
        LOGGER.info("Sinkin run subscribe msg on logical processor {}", Affinity.getCpu());

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

        // đọc các msg được gửi đến
        Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

        LOGGER.info("Sinkin start subscribe");

        try {
            // Nhận và xử lý tin nhắn
            while (_status == RUNNING) {
                byte[] msg = subscriber.recv(0);
                bytes.clear();
                bytes.write(msg);
                _ring_buffer_process_msg.publishEvent(
                        (newEvent, sequence, __bytesParam) -> {
                            newEvent.clear();
                            newEvent.setType(__bytesParam.readByte());
                            __bytesParam.read(newEvent.getData());
                        },
                        bytes);
            }
        } catch (Exception ex) {
            LOGGER.error("Sinkin SubMsg error", ex);
        } finally {
            bytes.release(_ref_id);
            subscriber.close();

            LOGGER.info("Sinkin end subscribe");
        }
    }


    /**
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void _onMsg(SinkProcessMsg event) {
        try {
            if (event.getType() == Constance.SINKIN.PROCESSS_MSG_TYPE.MSG) {
                // chỉ có 1 msg duy nhất

                _processOneMsg(event.getData(), true);
            } else if (event.getType() == Constance.SINKIN.PROCESSS_MSG_TYPE.MULTI_MSG) {
                // có thể có nhiều msg

                // đọc cho tới khi nào hết data thì thôi
                while (event.getData().readRemaining() > 0) {
                    _processOneMsg(event.getData(), false);
                }
            } else if (event.getType() == Constance.SINKIN.PROCESSS_MSG_TYPE.CHECK_MISS) {
                /*
                 * nếu hàng chờ còn msg và nó đã chờ quá lâu --> lấy các msg miss ở giữa
                 * nếu ko có msg chờ thì bỏ qua
                 */
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
            if (event.getType() == Constance.SINKIN.PROCESSS_MSG_TYPE.MSG || event.getType() == Constance.SINKIN.PROCESSS_MSG_TYPE.MULTI_MSG) {
                while (!_msg_wait.isEmpty() && _msg_wait.firstKey() == _seq_in_queue + 1) {
                    TranspotMsg tMsg = _msg_wait.remove(_msg_wait.firstKey());      // lấy msg chờ và xóa khỏi hàng chờ
                    _seq_in_queue++;
                    _src_latest_index = tMsg.getSrcIndex();
                    _appender.writeBytes(tMsg.toBytes());                           // ghi vào queue
                    _returnTranspotMsg(tMsg);                                       // trả lại về object pool
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Sinkin OnMsg error, msg {}", event.toString(), ex);
        }
    }


    /**
     * @param bytes   dữ liệu được bên source gửi về. Có thể chứa 1 hoặc nhiều bản ghi liên tiếp
     * @param onlyOne nếu dữ liệu trong 'bytes' chỉ có 1 bản ghi --> 'allData' chính là 'bytes' luôn
     */
    private void _processOneMsg(Bytes<ByteBuffer> bytes, boolean onlyOne) {
        try {
            // lấy từ object pool
            TranspotMsg tMsg = _getTranspotMsg();

            // deserialize data
            tMsg.clear();
            tMsg.setVersion(bytes.readByte());
            bytes.read(tMsg.getData(), bytes.readInt());
            tMsg.setSeq(bytes.readLong());
            tMsg.setSrcIndex(bytes.readLong());
            if (onlyOne) {
                tMsg.getAllData().write(bytes, 0, bytes.writePosition());
            }

            if (tMsg.getSeq() <= _seq_in_queue) {
                // msg đã xử lý thì bỏ qua
                // trả lại cho object pool
                _returnTranspotMsg(tMsg);
            } else if (tMsg.getSeq() == _seq_in_queue + 1) {
                // nếu là msg tiếp theo --> ghi vào trong queue
                _seq_in_queue++;
                _src_latest_index = tMsg.getSrcIndex();
                _appender.writeBytes(tMsg.toBytes());
                // trả lại cho object pool
                _returnTranspotMsg(tMsg);
            } else {
                // msg lớn hơn "current + 1" thì đẩy vào trong hệ thống chờ kèm thời gian
                tMsg.setRcvTime(System.currentTimeMillis());
                _msg_wait.put(tMsg.getSeq(), tMsg);
            }
        } catch (Exception ex) {
            LOGGER.error("Sinkin ProcessOneMsg error, onlyOne {}, bytes {}", onlyOne, Arrays.toString(bytes.toByteArray()), ex);
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

                    LOGGER.error("Get latest msg fail. Maybe TIMEOUT !");
                } else {
                    // nhận về
                    _byte_miss_msg_raw = _zSocket.recv();
                    if (_byte_miss_msg_raw == null) {
                        LOGGER.error("Rep latest msg empty. Maybe TIMEOUT !");
                        isSuccess = false;
                    } else if (_byte_miss_msg_raw.length > 0) {
                        _byte_miss_msg.write(_byte_miss_msg_raw);
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __type, __bytesParam) -> {
                                    newEvent.clear();
                                    newEvent.setType(__type);
                                    __bytesParam.read(newEvent.getData());
                                },
                                Constance.SINKIN.PROCESSS_MSG_TYPE.MSG,
                                _byte_miss_msg);
                    }
                    // clear msg
                    _byte_miss_msg.clear();
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
                    LOGGER.error(MessageFormat.format("Get items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getIndexFrom(), msg.getIndexTo()));
                } else {
                    // nhận về
                    _byte_miss_msg_raw = _zSocket.recv();
                    if (_byte_miss_msg_raw == null) {
                        LOGGER.error(MessageFormat.format("Rep items from {0} - to {1} fail. Maybe TIMEOUT !", msg.getIndexFrom(), msg.getIndexTo()));
                        isSuccess = false;
                    } else if (_byte_miss_msg_raw.length > 0) {
                        _byte_miss_msg.write(_byte_miss_msg_raw);
                        _ring_buffer_process_msg.publishEvent(
                                (newEvent, sequence, __type, __bytesParam) -> {
                                    newEvent.clear();
                                    newEvent.setType(__type);
                                    __bytesParam.read(newEvent.getData());
                                },
                                Constance.SINKIN.PROCESSS_MSG_TYPE.MULTI_MSG,
                                _byte_miss_msg);
                    }
                    // clear msg
                    _byte_miss_msg.clear();
                }
            }

            return isSuccess;
        } catch (Exception ex) {
            LOGGER.error("Sinkin OnMissMsgReq error, msg {}", msg.toString(), ex);
            return false;
        }
    }


    /**
     * lấy index của item cuối cùng trong queue
     * mục đích là để xác định vị trí cuối cùng đã đồng bộ từ Source từ đó tiếp tục đồng bộ
     */
    private long _getLatestIndex() {
        try {
            if (_queue.lastIndex() >= 0) {
                Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

                TranspotMsg latestItem = _getTranspotMsg(); // lấy 1 đối tượng từ pool

                ExcerptTailer tailer = _queue.createTailer();
                tailer.moveToIndex(_queue.lastIndex());
                tailer.readBytes(bytes);

                latestItem.clear();
                latestItem.setVersion(bytes.readByte());
                bytes.read(latestItem.getData(), bytes.readInt());
                latestItem.setSeq(bytes.readLong());
                latestItem.setSrcIndex(bytes.readLong());

                bytes.release(_ref_id);
                tailer.close();

                _returnTranspotMsg(latestItem); // trả lại đối tượng cho pool

                return latestItem.getSrcIndex();
            } else {
                return -1;
            }
        } catch (Exception ex) {
            LOGGER.error("Sinkin GetLatestIndex error", ex);
            return -1;
        }
    }


    /**
     * lắng nghe các event được viết vào queue và call cho "Handler"
     */
    private void _onWriteQueue() {
        ExcerptTailer tailer = _queue.createTailer();

        // di chuyển tới cuối
        tailer.toEnd();

        Bytes<ByteBuffer> byte_read = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> byte_msg_data = Bytes.elasticByteBuffer();
        Wire wire_msg_data = WireType.BINARY.apply(byte_msg_data);

        byte version;
        long seq;
        T objT = _eventFactory();
        long id;    // đưa ra cho người dùng bên ngoài đổi từ "index" sang "id"

        Runnable waiter = OmniWaitStrategy.getWaiter(_cfg.getQueueWaitStrategy());

        try {
            while (_status == RUNNING) {
                if (tailer.readBytes(byte_read)) {
                    version = byte_read.readByte();   // version
                    byte_read.read(byte_msg_data, byte_read.readInt()); // data
                    seq = byte_read.readLong();     // seq
                    id = byte_read.readLong();     // index

                    objT.readMarshallable(wire_msg_data);

                    _handler.apply(version, objT, seq, id);

                    byte_read.clear();
                    wire_msg_data.clear();
                    byte_msg_data.clear();
                } else {
                    waiter.run();
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Sinkin OnWriteQueue error", ex);
        } finally {
            byte_read.release(_ref_id);
            byte_msg_data.release(_ref_id);
            tailer.close();

            LOGGER.info("Sinkin subscribe write queue closed");
        }
    }


    // Tạo một instance mới của class được chỉ định
    private T _eventFactory() {
        try {
            return _dataType.newInstance();
        } catch (Exception ex) {
            LOGGER.error("Sinkin EventFactory error", ex);
            return null;
        }
    }


    //region OBJECT POOL
    // lấy đối tượng từ pool
    private TranspotMsg _getTranspotMsg() {
        try {
            return _object_pool.pop();
        } catch (Exception ex) {
            return new TranspotMsg();
        }
    }

    // trả lại đối tượng về cho pool
    private void _returnTranspotMsg(TranspotMsg msg) {
        _object_pool.push(msg);
    }
    //endregion


    /**
     * Lần lượt shutdown và giải phóng tài nguyên theo thứ tự từ đầu vào --> đầu ra
     * các công việc đang làm dở sẽ làm nốt cho xong
     */
    private void _onShutdown() {
        LOGGER.info("Sinkin closing...");

        _status = STOP;
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

        // close chronicle queue
        _appender.close();
        _queue.close();

        // object pool
        _object_pool.clear();

        // byte
        _byte_miss_msg.release(_ref_id);

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : _affinity_composes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(500_000_000);

        LOGGER.info("Sinkin CLOSED !");
    }


}
