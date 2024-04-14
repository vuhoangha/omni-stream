package io.github.vuhoangha.Example;

import net.openhft.chronicle.wire.SelfDescribingMarshallable;

// SelfDescribingMarshallable: khi class People này thêm hoặc bớt field thì dữ liệu vẫn có thể serialize/deserialize bình thường
// còn các kiểu serialize khác có thể gặp lỗi. Ví dụ như có nhiều kiểu serialize ko chứa field mà chỉ chứa value của các field
//     chúng được sắp xếp theo 1 thứ tự và khi giải mã cũng theo thứ tự đó
//     nhưng khi thêm/bớt field thì các thứ tự này ko còn đúng nữa và ko có cách nào trace lại
public class PeopleTest extends SelfDescribingMarshallable {
    private int index;
    private String name;

    // Constructors
    public PeopleTest() {
    }

    public PeopleTest(int index, String name) {
        this.index = index;
        this.name = name;
    }

    // Getters and setters
    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "People{" +
                "index=" + index +
                ", name='" + name + '\'' +
                '}';
    }
}
