package io.github.vuhoangha.ManyToOne;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import net.openhft.chronicle.wire.SelfDescribingMarshallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;


/**
 * Nhiều Snipper sẽ gửi dữ liệu để Collector thu thập
 */
public class Snipper<T extends SelfDescribingMarshallable> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Snipper.class);

    // config cho Snipper
    private final SnipperCfg _cfg;

    // map id của item với thời gian tối đa nó chờ bên Collector xác nhận
    private ConcurrentNavigableMap<Long, Long> _map_item_with_time = new ConcurrentSkipListMap<>();
    // map id của item với callback để call lại khi cần
    private ConcurrentHashMap<Long, CompletableFuture<Boolean>> _map_item_with_callback = new ConcurrentHashMap<>();
    // quản lý id của các request. ID sẽ increment sau mỗi request
    private AtomicLong _sequence_id = new AtomicLong(System.currentTimeMillis());

    //region DISRUPTOR
    // disruptor dùng để gom message từ nhiều thread lại và xử lý trong 1 thread duy nhất
    private final Disruptor<SnipperInterMsg> _disruptor_send_msg;
    private final RingBuffer<SnipperInterMsg> _ring_buffer_send_msg;
    //endregion


    // zeromq
    private final ZContext _zmq_context;
    // dùng để xử lý gửi/nhận sang Collector
    private final SnipperProcessor _processor;


    public Snipper(SnipperCfg cfg) throws Exception {
        _cfg = cfg;

        // validate
        if (cfg.getCollectorIP() == null)
            throw new Exception("Require collectorIP");
        if (cfg.getPort() == null)
            throw new Exception("Require port");

        // set default value
        if (cfg.getTimeout() == null)
            cfg.setTimeout(10000);
        if (cfg.getWaitStrategy() == null)
            cfg.setWaitStrategy(new BlockingWaitStrategy());
        if (cfg.getRingBufferSize() == null)
            cfg.setRingBufferSize(2 << 16);     // 131072

        /*
         * Sử dụng zeromq để gửi nhận dữ liệu giữa source <--> sink
         * zmq_context có thể sử dụng ở nhiều thread và nên chỉ có 1 cho mỗi process
         * socket chỉ nên sử dụng bởi 1 thread duy nhất để đảm bảo tính nhất quán
         */
        _zmq_context = new ZContext();

        /*
         * Gom nhiều message được gửi lại và chuyển cho 1 thread gửi đi
         * đồng thời nhận cả phản hồi từ Collector
         */
        _disruptor_send_msg = new Disruptor<>(
                SnipperInterMsg::new,
                cfg.getRingBufferSize(),
                Executors.newSingleThreadExecutor(),
                ProducerType.MULTI,
                cfg.getWaitStrategy());
        _disruptor_send_msg.start();
        _ring_buffer_send_msg = _disruptor_send_msg.getRingBuffer();
        _processor = new SnipperProcessor(
                _ring_buffer_send_msg,
                _zmq_context,
                _cfg.getUrl(),
                _map_item_with_time,
                _map_item_with_callback);
        new Thread(_processor).start();

        // được chạy khi JVM bắt đầu quá trình shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(this::_onShutdown));
    }


    public boolean send(T data) {
        try {
            long reqId = _sequence_id.incrementAndGet();

            // quản lý thời gian timeout
            _map_item_with_time.put(reqId, System.currentTimeMillis() + _cfg.getTimeout());

            // quản lý callback trả về
            CompletableFuture<Boolean> cb = new CompletableFuture<>();
            _map_item_with_callback.put(reqId, cb);

            // gửi sang luồng chính để gửi cho core
            _ring_buffer_send_msg.publishEvent(
                    (newEvent, sequence, __id, __data) -> {
                        newEvent.setId(__id);
                        newEvent.setData(__data);
                    },
                    reqId, data);

            return cb.get();
        } catch (Exception ex) {
            LOGGER.error("Snipper send error", ex);
            LOGGER.error("Data " + data.toString());

            return false;
        }
    }


    private void _onShutdown() {
        _map_item_with_time.clear();
        _map_item_with_callback.clear();

        // disruptor
        _disruptor_send_msg.shutdown();
        _processor.halt();                      // stop processor
        LockSupport.parkNanos(1_000_000_000);   // tạm ngừng để xử lý nốt msg trong ring buffer

        // zmq
        _zmq_context.destroy();
    }

}
