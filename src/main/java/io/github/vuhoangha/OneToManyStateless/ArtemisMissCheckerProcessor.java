package io.github.vuhoangha.OneToManyStateless;

import com.lmax.disruptor.*;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

/**
 * Class này có tác dụng lắng nghe các yêu cầu lấy msg bị miss từ Source
 * vì các yêu cầu này được gửi từ nhiều thread nên cần tổng hợp chúng lại 1 thread để xử lý
 * và cũng vì yêu cầu phải giữ cho ZMQ Socket chạy trong 1 thread duy nhất để đảm bảo tính đúng đắn và hiệu năng cao
 * học theo mô hình của 'BatchEventProcessor' trong Lmax Disruptor
 */
@Slf4j
public class ArtemisMissCheckerProcessor implements EventProcessor {

    private static final int IDLE = 0;              // trạng thái nằm im
    private static final int HALTED = IDLE + 1;     // trạng thái đã dừng
    private static final int RUNNING = HALTED + 1;  // trạng thái đang chạy

    private final AtomicInteger running = new AtomicInteger(IDLE);      // quản lý trạng thái hiện tại của processor

    private final long _ms_for_socket_ready = 3000;   // thời gian để socket sẵn sàng từ lúc khởi tạo

    private final RingBuffer<CheckMissMsgStateless> ringBuffer;
    private final Sequence sequence = new Sequence(Sequencer.INITIAL_CURSOR_VALUE);     // quản lý số thứ tự trong processor

    /*
     * dùng để lấy seq mới
     * check xem processor còn hoạt động không
     * nó sử dụng WaitStrategy như lúc ta thiết lập Disruptor bên ngoài
     */
    private final SequenceBarrier sequenceBarrier;


    //region LOGIC
    private final ZContext zContext;
    private final String zUrl;
    private final BiFunction<ZMQ.Socket, CheckMissMsgStateless, Boolean> eventHandler;
    //endregion


    //region CONFIG
    private final int timeoutSend;
    private final int timeoutRecv;
    //endregion


    public ArtemisMissCheckerProcessor(
            RingBuffer<CheckMissMsgStateless> ringBuffer,
            SequenceBarrier sequenceBarrier,
            ZContext zContext,
            String zUrl,
            int timeoutSend,
            int timeoutRecv,
            BiFunction<ZMQ.Socket, CheckMissMsgStateless, Boolean> eventHandler) {
        this.ringBuffer = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;

        this.zContext = zContext;
        this.zUrl = zUrl;
        this.eventHandler = eventHandler;

        this.timeoutSend = timeoutSend;
        this.timeoutRecv = timeoutRecv;
    }


    /**
     * bắt đầu chạy processor này
     * kỳ vọng nó chỉ chạy 1 lần duy nhất
     * thread sẽ chạy xuyên suốt và được nghỉ giữa chừng bởi WaitStrategy
     */
    @Override
    public void run() {
        log.info("ArtemisMissCheckerProcessor on logical processor {}", Affinity.getCpu());

        if (running.compareAndSet(IDLE, RUNNING)) {
            // start processor

            // xóa các alert đi. Nếu 1 alert được bật lên đồng nghĩa processor đang dừng
            sequenceBarrier.clearAlert();

            try {
                if (running.get() == RUNNING) {
                    _processEvents();
                }
            } finally {
                // sau khi chạy xong thì rơi vào trạng thái ngủ
                running.set(IDLE);
            }
        } else {
            // nếu processor đang chạy rồi thì throw error
            if (running.get() == RUNNING) {
                throw new IllegalStateException("Thread is already running");
            }
        }
    }


    /**
     * luồng xử lý cốt lõi ở đây
     * lắng nghe 1 seq mới
     * khi ring_buffer trả về seq cuối cùng trong ring_buffer
     * ta xử lý theo batch lần lượt các event từ vị trí cuối cùng processor xử lý tới vị trí cuối cùng trong ring_buffer hiện đang có
     * sau đó processor update lại ví trí cuối cùng nó xử lý
     */
    private void _processEvents() {
        CheckMissMsgStateless event = null;
        long nextSequence = sequence.get() + 1L;
        ZMQ.Socket zSocket = _createSocket();
        long socketReadyTime = System.currentTimeMillis() + _ms_for_socket_ready;   // thời gian để socket sẵn sàng xử lý
        long availableSequence;

        try {
            while (true) {
                try {
                    availableSequence = sequenceBarrier.waitFor(nextSequence);

                    while (nextSequence <= availableSequence) {
                        event = ringBuffer.get(nextSequence);

                        if (socketReadyTime < System.currentTimeMillis()) {    // socket phải sẵn sàng thì mới xử lý
                            boolean handlerSuccess = eventHandler.apply(zSocket, event);
                            if (!handlerSuccess) {      // nếu quá trình gửi bị timeout --> close socket --> connect lại
                                zSocket.close();
                                zSocket = _createSocket();
                                socketReadyTime = System.currentTimeMillis() + _ms_for_socket_ready;
                            }
                        }

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
            log.error("ArtemisMissCheckerProcessor ProcessEvents error", ex);
        } finally {
            zSocket.close();
        }
    }

    private ZMQ.Socket _createSocket() {
        ZMQ.Socket zSocket = zContext.createSocket(SocketType.REQ);
        zSocket.setSendTimeOut(this.timeoutSend);
        zSocket.setReceiveTimeOut(this.timeoutRecv);
        zSocket.connect(zUrl);
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
