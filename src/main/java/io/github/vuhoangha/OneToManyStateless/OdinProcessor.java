package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.*;
import io.github.vuhoangha.Common.Utils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class OdinProcessor implements EventProcessor {

    private static final int IDLE = 0;              // trạng thái nằm im
    private static final int HALTED = IDLE + 1;     // trạng thái đã dừng
    private static final int RUNNING = HALTED + 1;  // trạng thái đang chạy
    private final AtomicInteger running = new AtomicInteger(IDLE);      // quản lý trạng thái hiện tại của processor

    private final RingBuffer<OdinDisruptorEvent> ringBuffer;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);     // quản lý số thứ tự trong processor

    /*
     * dùng để lấy seq mới
     * check xem processor còn hoạt động không
     * nó sử dụng WaitStrategy như lúc ta thiết lập Disruptor bên ngoài
     */
    private final SequenceBarrier sequenceBarrier;
    private final ZContext zContext;
    private final OdinCfg odinCfg;
    private final int port;


    public OdinProcessor(
            RingBuffer<OdinDisruptorEvent> ringBuffer,
            SequenceBarrier sequenceBarrier,
            ZContext zContext,
            int port,
            OdinCfg odinCfg) {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.zContext = zContext;
        this.odinCfg = odinCfg;
        this.port = port;
    }


    /**
     * bắt đầu chạy processor này
     * kỳ vọng nó chỉ chạy 1 lần duy nhất
     * thread sẽ chạy xuyên suốt và được nghỉ giữa chừng bởi WaitStrategy
     */
    @Override
    public void run() {
        log.info("OdinProcessor on logical processor {}", Affinity.getCpu());

        if (running.compareAndSet(IDLE, RUNNING)) {
            // xóa các alert đi. Nếu 1 alert được bật lên đồng nghĩa processor đang dừng
            sequenceBarrier.clearAlert();

            try {
                if (running.get() == RUNNING)
                    Utils.runWithThreadAffinity(
                            "Odin Processor port " + this.port,
                            false,
                            odinCfg.getEnableBindingCore(),
                            odinCfg.getCpu(),
                            false,
                            odinCfg.getDisruptorHandlerCpu(),
                            this::_process);
            } finally {
                // sau khi chạy xong thì rơi vào trạng thái ngủ
                running.set(IDLE);
            }
        } else {
            // nếu processor đang chạy rồi thì throw error
            if (running.get() == RUNNING)
                throw new IllegalStateException("Thread is already running");
        }
    }


    /**
     * luồng xử lý cốt lõi ở đây
     * lắng nghe 1 seq mới
     * khi ring_buffer trả về seq cuối cùng trong ring_buffer
     * ta xử lý theo batch lần lượt các event từ vị trí cuối cùng processor xử lý tới vị trí cuối cùng trong ring_buffer hiện đang có
     * sau đó processor update lại ví trí cuối cùng nó xử lý
     */
    private void _process() {
        OdinDisruptorEvent event = null;
        long nextSequence = sequence.get() + 1L;
        ZMQ.Socket zSocket = _createSocket();
        long availableSequence;

        try {
            while (true) {
                try {
                    availableSequence = sequenceBarrier.waitFor(nextSequence);

                    while (nextSequence <= availableSequence) {
                        event = ringBuffer.get(nextSequence);
                        zSocket.send(event.getBinary());
                        nextSequence++;
                    }

                    sequence.set(availableSequence);
                } catch (final TimeoutException e) {
                    // bị timeout khi xử lý
                    // notifyTimeout(sequence.get());
                } catch (final AlertException ex) {
                    // processor đang ko chạy thì stop luôn tiến trình này lại
                    if (running.get() != RUNNING) {
                        break;
                    }
                } catch (final Throwable ex) {
                    // các lỗi khác ko handler được thì bỏ qua event này, xử lý cái tiếp theo
                    sequence.set(nextSequence);
                    nextSequence++;
                }
            }
        } catch (Exception ex) {
            log.error("OdinProcessor ProcessEvents error", ex);
        } finally {
            zSocket.close();
        }
    }

    private ZMQ.Socket _createSocket() {
        ZMQ.Socket zSocket = zContext.createSocket(SocketType.PUB);
        zSocket.setSndHWM(odinCfg.getMaxNumberMsgInCachePub()); // Thiết lập HWM cho socket. Default = 1000
        zSocket.setHeartbeatIvl(10000);
        zSocket.setHeartbeatTtl(15000);
        zSocket.setHeartbeatTimeout(15000);
        zSocket.bind("tcp://*:" + port);
        return zSocket;
    }

    @Override
    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public boolean isRunning() {
        return running.get() != IDLE;
    }

    @Override
    public void halt() {
        running.set(HALTED);
        sequenceBarrier.alert();
    }
}