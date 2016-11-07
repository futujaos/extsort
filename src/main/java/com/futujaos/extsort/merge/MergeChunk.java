package com.futujaos.extsort.merge;

public interface MergeChunk<T> {

    T readElement() throws Exception;
}