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
import java.util.concurrent.locks.LockSupport;

/*
 * lưu vào queue:
 *      - compress true: ["src native index"]["sequence"]["compress_data"]
 *      - compress false: ["src native index"]["sequence"]["data"]
 */
@Slf4j
public class Sinkin {

    private enum SinkStatus {IDLE, SYNCING, RUNNING, STOP}

    private SinkStatus status;                                                            // quản lý trạng thái của Sinkin

    ScheduledExecutorService checkMsgExecutor = Executors.newScheduledThreadPool(1);
    private final NavigableMap<Long, TranspotMsg> waitingMessages = new TreeMap<>();      // TODO đoạn này xem xét tự viết 1 kiểu dữ liệu tương tự nhưng performance tốt hơn
    private final SynchronizeObjectPool<TranspotMsg> messagePool;
    private final SinkinCfg config;
    private final SingleChronicleQueue chronicleQueue;
    private final ExcerptAppender chronicleAppender;
    private long latestWriteSequence;                                                     // số thứ tự của msg trong queue. Được đánh bởi bên Fanout
    private long latestWriteIndex;                                                        // src index của item mới nhất trong queue
    private final ZContext zmqContext;
    private final Bytes<ByteBuffer> missMsgBytes = Bytes.elasticByteBuffer();             // dùng để tạo data lấy msg miss
    private final Bytes<ByteBuffer> processMsgBytes = Bytes.elasticByteBuffer();          // dùng để tạo data xử lý msg
    private Disruptor<SinkProcessMsg> processMsgDisruptor;
    private RingBuffer<SinkProcessMsg> processMsgRingBuffer;
    private Disruptor<CheckMissMsg> missMsgDisruptor;
    private RingBuffer<CheckMissMsg> missMsgRingBuffer;
    private SinkinMissCheckerProcessor missCheckerProcessor;
    List<AffinityCompose> affinityComposes = Collections.synchronizedList(new ArrayList<>());
    private final SinkinHandler messageHandler;
    private long endSyncedSeq = -1;                                                       // sequence kết thúc việc đồng bộ dữ liệu ban đầu từ Fanout


    public Sinkin(SinkinCfg cfg, SinkinHandler handler) {
        // validate
        Utils.checkNull(cfg.getQueuePath(), "Require queuePath");
        Utils.checkNull(cfg.getSourceIP(), "Require source IP");
        Utils.checkNull(handler, "Require handler");
        Utils.checkNull(cfg.getReaderName(), "Require readerName");

        config = cfg;
        messageHandler = handler;
        messagePool = new SynchronizeObjectPool<>(new TranspotMsg[cfg.getMaxObjectsPoolWait()], TranspotMsg::new);
        zmqContext = new ZContext();

        chronicleQueue = SingleChronicleQueueBuilder
                .binary(config.getQueuePath())
                .rollCycle(config.getRollCycles())
                .build();
        chronicleAppender = chronicleQueue.acquireAppender();

        LongLongObject lastestInfo = getLatestInfo();

        if (lastestInfo != null) {
            latestWriteIndex = lastestInfo.valueA;
            if (lastestInfo.valueB != chronicleQueue.entryCount()) {
                throw new RuntimeException(MessageFormat.format("Sequence in queue not match, latestInfo {0}, entryCount {1}", lastestInfo.valueB, chronicleQueue.entryCount()));
            } else {
                latestWriteSequence = chronicleQueue.entryCount();   // tổng số item trong queue
            }
        } else {
            latestWriteIndex = -1;
            latestWriteSequence = 0;
        }

        status = SinkStatus.SYNCING;
        LockSupport.parkNanos(100_000_000L);

        // chạy đồng bộ dữ liệu với source trước
        new Thread(this::sync).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::onShutdown));
    }


    /**
     * Hàm này sẽ đồng bộ hoàn toàn msg từ src --> sink. Khi đã hoàn thành thì mới sub msg mới
     * cấu trúc request: ["type"]["src index"]
     * Cấu trúc response: ["msg_1"]["msg_2"]...vvv
     * cấu trúc từng msg con msg_1, msg_2..vv:  ["độ dài data 1]["source native index 1"]["sequence 1"]["data nén 1"]["độ dài data 2]["source native index 2"]["sequence 2"]["data nén 2"]
     */
    private void sync() {
        ZMQ.Socket socket = zmqContext.createSocket(SocketType.REQ);
        socket.connect(config.getConfirmUrl());

        ExcerptAppender localAppender = chronicleQueue.acquireAppender();
        Bytes<ByteBuffer> byteRes = Bytes.elasticByteBuffer();              // chứa byte[] của tất cả bản ghi trả về
        Bytes<ByteBuffer> byteToQueue = Bytes.elasticByteBuffer();          // chứa byte[] để ghi vào queue

        try {
            while (status == SinkStatus.SYNCING) {
                // tổng hợp data rồi req sang src
                missMsgBytes.writeByte(Constance.FANOUT.CONFIRM.FROM_LATEST);
                missMsgBytes.writeLong(latestWriteIndex);
                socket.send(missMsgBytes.toByteArray(), 0);
                missMsgBytes.clear();

                // chờ dữ liệu trả về
                byte[] repData = socket.recv(0);

                // nếu đã hết msg thì thôi
                if (repData.length == 0) break;

                log.info("Sinkin syncing......");

                // đọc tuần tự và xử lý
                byteRes.write(repData);

                while (byteRes.readRemaining() > 0) {
                    // cấu trúc data ["độ dài data]["source native index"]["sequence"]["data"][

                    // độ dài dữ liệu nén
                    int dataLen = byteRes.readInt();

                    // lấy phần dữ liệu để ghi vào queue
                    byteToQueue.write(byteRes, byteRes.readPosition(), 8 + 8 + dataLen);

                    long srcIndex = byteRes.readLong();
                    long seq = byteRes.readLong();
                    byteRes.readSkip(dataLen);              // bỏ qua data chính

                    if (seq != latestWriteSequence + 1) {
                        log.error("Sinkin _sync not sequence, src_seq: {}, sink_seq: {}", seq, latestWriteSequence);
                        return;
                    }

                    // update seq và native index
                    latestWriteSequence = seq;
                    latestWriteIndex = srcIndex;

                    // write to queue
                    localAppender.writeBytes(byteToQueue);

                    // clear temp data
                    byteToQueue.clear();
                }

                byteRes.clear();
            }

            log.info("Sinkin synced");

            endSyncedSeq = latestWriteSequence;
            status = SinkStatus.RUNNING;
            LockSupport.parkNanos(100_000_000L);

            // khởi tạo luồng chính
            affinityComposes.add(
                    Utils.runWithThreadAffinity(
                            "Sinkin ALL",
                            true,
                            config.getEnableBindingCore(),
                            config.getCpu(),
                            config.getEnableBindingCore(),
                            config.getCpu(),
                            this::mainProcess));
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


    // luồng xử lý logic chính sau khi đã đồng bộ dữ liệu xong
    private void mainProcess() {

        log.info("Sinkin run main flow on logical processor {}", Affinity.getCpu());

        // khởi tạo disruptor xử lý các msg chính
        affinityComposes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Disruptor Process Msg",
                        false,
                        config.getEnableBindingCore(),
                        config.getCpu(),
                        config.getEnableDisruptorProcessMsgBindingCore(),
                        config.getDisruptorProcessMsgCpu(),
                        this::initDisruptorProcessMsg));

        // định kỳ kiểm tra các msg bị miss hoặc chờ quá lâu không
        this.initCheckMissMsg();

        // lắng nghe các msg được ghi vào queue và chuyển nó tới ứng dụng
        affinityComposes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Sub Queue",
                        false,
                        config.getEnableBindingCore(),
                        config.getCpu(),
                        config.getEnableCheckMissMsgAndSubQueueBindingCore(),
                        config.getCheckMissMsgAndSubQueueCpu(),
                        () -> new Thread(this::onWriteQueue).start()));

        // lắng nghe các msg Fanout gửi sang và đẩy vào xử lý rồi lưu vào queue
        affinityComposes.add(
                Utils.runWithThreadAffinity(
                        "Sinkin Sub Msg",
                        false,
                        config.getEnableBindingCore(),
                        config.getCpu(),
                        config.getEnableSubMsgBindingCore(),
                        config.getSubMsgCpu(),
                        () -> new Thread(this::subMsg).start()));
    }


    /**
     * xử lý các msg được gửi tới và lưu vào queue
     * sau đó check xem còn msg chờ trong cache ko thì lấy ra xử lý
     * Nhận xử lý từ các nơi sau:
     * các msg đến từ pub/sub
     * các miss msg
     * các yêu cầu check miss msg
     */
    private void initDisruptorProcessMsg() {
        log.info("Sinkin run disruptor process msg on logical processor {}", Affinity.getCpu());

        processMsgDisruptor = new Disruptor<>(
                SinkProcessMsg::new,
                config.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                config.getWaitStrategy());
        processMsgDisruptor.handleEventsWith((event, sequence, endOfBatch) -> this.onMsg(event));
        processMsgDisruptor.start();
        processMsgRingBuffer = processMsgDisruptor.getRingBuffer();
    }


    /**
     * Check các msg bị miss
     */
    private void initCheckMissMsg() {

        /*
         * Check xem có msg nào bị miss ko
         * Có 2 chế độ là "lấy msg mới nhất" và "lấy msg nằm giữa 2 index"
         */
        missMsgDisruptor = new Disruptor<>(
                CheckMissMsg::new,
                2 << 7,    // 256
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                new BlockingWaitStrategy());
        missMsgDisruptor.start();
        missMsgRingBuffer = missMsgDisruptor.getRingBuffer();

        // lắng nghe các yêu cầu lấy msg bị miss
        missCheckerProcessor = new SinkinMissCheckerProcessor(
                missMsgRingBuffer,
                missMsgRingBuffer.newBarrier(),
                zmqContext,
                config.getConfirmUrl(),
                config.getTimeoutSendReqMissMsg(),
                config.getTimeoutRecvReqMissMsg(),
                this::onMissMsgReq);
        new Thread(missCheckerProcessor).start();

        /*
         * định kỳ check xem có msg mới ko
         * định kỳ check xem msg trong hàng đợi có chờ quá lâu ko
         */
        checkMsgExecutor.scheduleAtFixedRate(this::checkLatestMsg, 1000, config.getTimeRateGetLatestMsgMS(), TimeUnit.MILLISECONDS);
        checkMsgExecutor.scheduleAtFixedRate(this::checkLongTimeMsg, 10, config.getTimeRateGetMissMsgMS(), TimeUnit.MILLISECONDS);
    }


    /**
     * định kỳ lấy bản ghi mới nhất từ queue về
     * trong luồng chính xử lý msg, nếu bản ghi này đã tồn tại thì bỏ qua
     * nếu là bản ghi tiếp theo thì xử lý
     * nếu là bản ghi xa hơn nữa thì để vào trong hàng chờ
     * nếu lâu quá chưa được xử lý thì sẽ có "checkLongTimeMsg" định kỳ check để lấy ra xử lý
     */
    private void checkLatestMsg() {
        missMsgRingBuffer.publishEvent(
                (newEvent, sequence, __type) -> newEvent.setType(__type),
                Constance.FANOUT.CONFIRM.LATEST_MSG);
    }


    /**
     * đẩy vào trong luồng xử lý msg chính để lấy ra các msg đã vào nhưng lâu được xử lý
     */
    private void checkLongTimeMsg() {
        processMsgRingBuffer.publishEvent(
                (newEvent, sequence, __type) -> newEvent.setType(__type),
                Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS);
    }


    /**
     * Sub các msg được src stream sang
     */
    private void subMsg() {
        log.info("Sinkin run subscribe msg on logical processor {}", Affinity.getCpu());

        ZMQ.Socket subscriber = zmqContext.createSocket(SocketType.SUB);
        subscriber.setRcvHWM(config.getZmqSubBufferSize());   // setting buffer size các msg được nhận

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

        subscriber.connect(config.getRealTimeUrl());
        subscriber.subscribe(ZMQ.SUBSCRIPTION_ALL);     // nhận tất cả tin nhắn từ publisher

        log.info("Sinkin start subscribe");

        // Nhận và xử lý tin nhắn
        while (status == SinkStatus.RUNNING) {
            try {
                byte[] msg = subscriber.recv(0);
                processMsgRingBuffer.publishEvent(
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
    private void onMsg(SinkProcessMsg event) {
        try {
            if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MSG) {
                // chỉ có 1 msg duy nhất. Cấu trúc ["msg type"]["source native index"]["seq in queue"]["data"]

                processMsgBytes.write(event.getData());

                TranspotMsg tMsg = messagePool.pop();

                tMsg.getQueueData().write(processMsgBytes, 1, processMsgBytes.readLimit() - 1);

                processMsgBytes.readSkip(1);       // byte đầu ko cần nên bỏ
                tMsg.setSrcIndex(processMsgBytes.readLong());
                tMsg.setSeq(processMsgBytes.readLong());
                processMsgBytes.read(tMsg.getData(), (int) processMsgBytes.readRemaining());

                processOneMsg(tMsg);

                processMsgBytes.clear();
            } else if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MULTI_MSG) {
                // có thể có nhiều msg

                processMsgBytes.write(event.getData());

                // đọc cho tới khi nào hết data thì thôi
                while (processMsgBytes.readRemaining() > 0) {
                    TranspotMsg tMsg = messagePool.pop();

                    // Cấu trúc: ["độ dài data 1"]["source native index 1"]["seq in queue 1"]["data 1"]["độ dài data 2"]["source native index 2"]["seq in queue 2"]["data 2"]
                    int dataLen = processMsgBytes.readInt();
                    tMsg.getQueueData().write(processMsgBytes, processMsgBytes.readPosition(), 8 + 8 + dataLen);    // đọc nhưng ko ảnh hưởng readPosition trong _byte_process_msg
                    tMsg.setSrcIndex(processMsgBytes.readLong());
                    tMsg.setSeq(processMsgBytes.readLong());
                    processMsgBytes.read(tMsg.getData(), dataLen);

                    processOneMsg(tMsg);
                }

                processMsgBytes.clear();
            } else if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.CHECK_MISS) {
                // nếu hàng chờ còn msg và nó đã chờ quá lâu --> lấy các msg miss ở giữa
                // nếu ko có msg chờ thì bỏ qua
                if (!waitingMessages.isEmpty()) {
                    TranspotMsg tMsg = waitingMessages.firstEntry().getValue();
                    if (tMsg.getRcvTime() + config.getMaxTimeWaitMS() < System.currentTimeMillis()) {
                        missMsgRingBuffer.publishEvent(
                                (newEvent, sequence, __type, __indexFrom, __indexTo) -> {
                                    newEvent.setType(__type);
                                    newEvent.setIndexFrom(__indexFrom);
                                    newEvent.setIndexTo(__indexTo);
                                },
                                Constance.FANOUT.CONFIRM.FROM_TO,
                                latestWriteIndex,
                                tMsg.getSrcIndex());
                    }
                }
            }

            // check xem trong hàng đợi có msg kế tiếp không ?
            if (event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MSG || event.getType() == Constance.SINKIN.PROCESS_MSG_TYPE.MULTI_MSG) {
                while (!waitingMessages.isEmpty() && waitingMessages.firstKey() <= latestWriteSequence + 1) {
                    long firstKey = waitingMessages.firstKey();
                    TranspotMsg tMsg = waitingMessages.remove(firstKey);      // lấy msg chờ và xóa khỏi hàng chờ
                    if (firstKey == latestWriteSequence + 1) {
                        latestWriteSequence = tMsg.getSeq();
                        latestWriteIndex = tMsg.getSrcIndex();
                        chronicleAppender.writeBytes(tMsg.getQueueData());                           // ghi vào queue
                    }

                    // clear data và trả về pool
                    tMsg.clear();
                    messagePool.push(tMsg);
                }
            }
        } catch (Exception ex) {
            log.error("Sinkin OnMsg error", ex);
        }
    }


    // xử lý 1 msg hoàn chỉnh
    private void processOneMsg(TranspotMsg tMsg) {
        try {
            if (tMsg.getSeq() <= latestWriteSequence) {
                // msg đã xử lý thì bỏ qua
                // trả về pool
                tMsg.clear();
                messagePool.push(tMsg);
            } else if (tMsg.getSeq() == latestWriteSequence + 1) {
                // nếu là msg tiếp theo --> ghi vào trong queue
                latestWriteSequence = tMsg.getSeq();
                latestWriteIndex = tMsg.getSrcIndex();
                chronicleAppender.writeBytes(tMsg.getQueueData());
                // trả về pool
                tMsg.clear();
                messagePool.push(tMsg);
            } else {
                // msg lớn hơn "current + 1" thì đẩy vào trong hệ thống chờ kèm thời gian
                tMsg.setRcvTime(System.currentTimeMillis());
                waitingMessages.put(tMsg.getSeq(), tMsg);
            }
        } catch (Exception ex) {
            // trả về pool
            tMsg.clear();
            messagePool.push(tMsg);
            log.error("Sinkin ProcessOneMsg error", ex);
        }
    }


    /**
     * lấy msg mới nhất của src
     * xử lý các yêu cầu lấy msg bị miss từ [index_from, index_to]
     * trả về "true" nếu việc gửi/nhận thành công và ngược lại
     */
    private boolean onMissMsgReq(ZMQ.Socket _zSocket, CheckMissMsg msg) {
        try {
            boolean isSuccess = true;

            if (msg.getType() == Constance.FANOUT.CONFIRM.LATEST_MSG) {
                // nếu lấy msg cuối cùng

                missMsgBytes.writeByte(Constance.FANOUT.CONFIRM.LATEST_MSG);

                // gửi đi
                isSuccess = _zSocket.send(missMsgBytes.toByteArray());
                missMsgBytes.clear();

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
                        processMsgRingBuffer.publishEvent(
                                (newEvent, sequence, __bytesData) -> newEvent.setType(__bytesData[0]).setData(__bytesData),
                                resData);
                    }
                }
            } else if (msg.getType() == Constance.FANOUT.CONFIRM.FROM_TO) {
                // nếu lấy các bản ghi from-to index

                missMsgBytes.writeByte(Constance.FANOUT.CONFIRM.FROM_TO);
                missMsgBytes.writeLong(msg.getIndexFrom());
                missMsgBytes.writeLong(msg.getIndexTo());

                // gửi đi
                isSuccess = _zSocket.send(missMsgBytes.toByteArray());
                missMsgBytes.clear();

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
                        processMsgRingBuffer.publishEvent(
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
     * valueA: src native index
     * valueB: sequence
     */
    private LongLongObject getLatestInfo() {
        try {
            if (chronicleQueue.lastIndex() >= 0) {
                Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer();

                ExcerptTailer tailer = chronicleQueue.createTailer();
                tailer.moveToIndex(chronicleQueue.lastIndex());
                tailer.readBytes(bytes);

                LongLongObject result = new LongLongObject(bytes.readLong(), bytes.readLong());

                bytes.releaseLast();

                return result;
            }
        } catch (Exception ex) {
            log.error("Sinkin GetLatestIndex error", ex);
        }
        return null;
    }


    /**
     * lắng nghe các event được viết vào queue và call cho "Handler"
     */
    private void onWriteQueue() {

        log.info("Sinkin listen queue on logical processor {}", Affinity.getCpu());

        Bytes<ByteBuffer> bytesRead = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesDecompress = Bytes.elasticByteBuffer();
        Bytes<ByteBuffer> bytesData = Bytes.elasticByteBuffer();

        // tạo 1 tailer. Mặc định nó sẽ đọc từ lần cuối cùng nó đọc
        ExcerptTailer tailer = chronicleQueue.createTailer(config.getReaderName());
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
                log.error("Collection tailer fail because invalid index {}", config.getStartId());
            }
        }

        Runnable waiter = OmniWaitStrategy.getWaiter(config.getQueueWaitStrategy());

        while (status == SinkStatus.RUNNING || status == SinkStatus.SYNCING) {
            try {
                if (tailer.readBytes(bytesRead)) {
                    bytesRead.readSkip(8);      // bỏ qua src index
                    long seq = bytesRead.readLong();

                    if (config.getCompress()) {
                        // giải nén
                        byte[] compressedBytes = new byte[(int) bytesRead.readRemaining()];
                        bytesRead.read(compressedBytes); // Đọc toàn bộ dữ liệu nén của msg này
                        byte[] decompressData = Lz4Compressor.decompressData(compressedBytes);
                        bytesDecompress.write(decompressData);

                        messageHandler.apply(tailer.lastReadIndex(), seq, endSyncedSeq, bytesDecompress);
                    } else {
                        bytesRead.read(bytesData, (int) bytesRead.readRemaining());
                        messageHandler.apply(tailer.lastReadIndex(), seq, endSyncedSeq, bytesData);
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
    private void onShutdown() {
        log.info("Sinkin closing...");

        status = SinkStatus.STOP;
        LockSupport.parkNanos(500_000_000);

        // close zeromq, ngừng nhận msg mới
        zmqContext.destroy();

        // turnoff miss check processor, ngừng việc lấy các msg thiếu và msg mới nhất
        checkMsgExecutor.shutdownNow();
        missCheckerProcessor.halt();
        missMsgDisruptor.shutdown();

        // close disruptor, ngừng nhận msg mới, xử lý nốt msg trong ring_buffer
        processMsgDisruptor.shutdown();
        // ngừng 2s để xử lý nốt msg trong ring buffer
        LockSupport.parkNanos(1_000_000_000);

        // close chronicle queue
        chronicleAppender.close();
        chronicleQueue.close();

        // object pool
        messagePool.clear();

        // byte
        missMsgBytes.releaseLast();

        // giải phóng các CPU core / Logical processor đã sử dụng
        for (AffinityCompose affinityCompose : affinityComposes) {
            affinityCompose.release();
        }

        LockSupport.parkNanos(500_000_000);

        log.info("Sinkin CLOSED !");
    }


}
