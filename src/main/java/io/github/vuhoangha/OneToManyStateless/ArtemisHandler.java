package io.github.vuhoangha.OneToManyStateless;

public interface ArtemisHandler<T> {

    void apply(long version, long seq, T data);

}
