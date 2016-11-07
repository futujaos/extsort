package com.futujaos.extsort.merge.impl;

import com.futujaos.extsort.merge.MergeLogger;

public class MergeLoggerImpl implements MergeLogger {
    private final int logStep;

    public MergeLoggerImpl(int logStep) {
        this.logStep = logStep;
    }

    @Override
    public void logIfNeeded(int elementsMerged) {
        if (elementsMerged % logStep == 0) {
            System.out.println(elementsMerged + " numbers written to target file");
        }
    }
}