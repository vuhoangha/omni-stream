package io.github.vuhoangha.OneToMany;

public interface SinkinHandler<T> {

    void apply(byte version, T data, long seq, long id);

}
