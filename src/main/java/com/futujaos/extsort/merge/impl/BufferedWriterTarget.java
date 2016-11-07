package com.futujaos.extsort.merge.impl;

import com.futujaos.extsort.merge.MergeTarget;

import java.io.BufferedWriter;
import java.util.Objects;

public final class BufferedWriterTarget implements MergeTarget<Integer> {
    private final BufferedWriter writer;

    public BufferedWriterTarget(BufferedWriter writer) {
        this.writer = writer;
    }

    @Override
    public void writeElement(Integer element) throws Exception {
        Objects.requireNonNull(element);

        writer.write(String.valueOf(element));
        writer.write("\n");
    }

    public BufferedWriter getWriter() {
        return writer;
    }
}