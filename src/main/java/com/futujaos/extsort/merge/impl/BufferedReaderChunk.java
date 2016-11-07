package com.futujaos.extsort.merge.impl;

import com.futujaos.extsort.merge.MergeChunk;

import java.io.BufferedReader;

public class BufferedReaderChunk implements MergeChunk<Integer> {
    private final BufferedReader reader;

    public BufferedReaderChunk(BufferedReader reader) {
        this.reader = reader;
    }

    @Override
    public Integer readElement() throws Exception {
        final String line = reader.readLine();
        return line == null ? null : Integer.valueOf(line);
    }

    public BufferedReader getReader() {
        return reader;
    }
}