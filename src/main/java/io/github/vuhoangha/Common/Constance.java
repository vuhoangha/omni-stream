package io.github.vuhoangha.Common;

public class Constance {


    public static class FANOUT {

        public static class FETCH {
            public static byte LATEST_MSG = 1;      // lấy msg mới nhất
            public static byte FROM_LATEST = 2;     // lấy các bản ghi từ hiện tại tới mới nhất hoặc chạm limit số bản ghi được lấy 1 lần
            public static byte FROM_TO = 3;         // lấy các bản ghi từ index_from tới index_to nhưng vẫn <= limit
        }

    }


    public static class SINKIN {
        // các loại data đầu vào cho quá trình xử lý chính
        public static class PROCESS_MSG_TYPE {
            public static byte CHECK_MISS = 3;                                  // kiểm tra xem có msg nào miss trong hàng chờ ko
            public static byte MULTI_MSG = 4;                                   // loại này chứa nhiều msg con ở trong, xử lý kiểu khác để tối ưu hơn
        }
    }


    public static class ODIN {
        public static class CONFIRM {
            public static byte LATEST_MSG = 1;      // lấy msg mới nhất
            public static byte FROM_TO = 3;         // lấy các bản ghi từ seq_from tới seq_to nhưng vẫn <= limit
        }
    }


    public static class ARTEMIS {
        // các loại data đầu vào cho quá trình xử lý chính
        public static class PROCESSS_MSG_TYPE {
            public static byte MSG = 1;                                         // xử lý msg queue mới
            public static byte HEART_BEAT = 2;                                  // msg heartbeat để kiểm tra connect giữa sink và src còn hoạt động ko
            public static byte CHECK_MISS = 3;                                  // kiểm tra xem có msg nào miss trong hàng chờ ko
            public static byte MULTI_MSG = 4;                                   // loại này chứa nhiều msg con ở trong, xử lý kiểu khác để tối ưu hơn
            public static byte INIT = 5;                                        // khi bắt đầu subscribe msg từ Odin thì seq sẽ thường ko bằng "0". Do đó ta cần lấy seq có giá trị nhỏ nhất
        }
    }


    // các kiểu logical processor
    public static class CPU_TYPE {
        public static Integer ANY = -1;         // gán bất kỳ
        public static Integer NONE = -2;        // ko gán cho 1 CPU core / logical processor cụ thể nào
    }

}
