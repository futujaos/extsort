package com.futujaos.extsort.merge.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.futujaos.extsort.merge.MergeLogger;

public class MergeLoggerImpl implements MergeLogger {
    private static final Logger logger = LogManager.getLogger();
    private final int logStep;

    public MergeLoggerImpl(int logStep) {
        this.logStep = logStep;
    }

    @Override
    public void logIfNeeded(int elementsMerged) {
        if (elementsMerged % logStep == 0) {
            logger.info(elementsMerged + " numbers written to target file");
        }
    }
}