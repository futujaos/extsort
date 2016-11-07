package com.futujaos.extsort.merge;

public interface MergeTarget<T> {

    void writeElement(T element) throws Exception;
}