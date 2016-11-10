package com.futujaos.extsort;

import com.futujaos.extsort.merge.Merger;
import com.futujaos.extsort.merge.impl.BufferedReaderChunk;
import com.futujaos.extsort.merge.impl.BufferedWriterTarget;
import com.futujaos.extsort.merge.impl.MergeLoggerImpl;
import com.futujaos.extsort.split.Splitter;

import java.io.*;
import java.util.*;

public class Extsort {
    private static final int DEFAULT_MAX_NUMBERS_IN_CHUNK = 1 << 24; // 16M
    private final File sourceFile;
    private final File targetFile;

    public Extsort(File sourceFile, File targetFile) {
        this.sourceFile = sourceFile;
        this.targetFile = targetFile;
    }

    public void sort() throws IOException {
        final Splitter splitter = new Splitter(sourceFile, DEFAULT_MAX_NUMBERS_IN_CHUNK);
        final List<File> chunks = splitter.split();
        join(chunks);
        cleanup(chunks);
        System.out.println("Done!");
    }

    private void join(List<File> chunks) throws IOException {
        System.out.println("Joining " + chunks.size() + " chunks to target file...");

        final List<BufferedReaderChunk> chunkReaders = new ArrayList<>(chunks.size());
        BufferedWriterTarget targetWriter = null;
        try {
            for (File chunk : chunks) {
                chunkReaders.add(new BufferedReaderChunk(new BufferedReader(new FileReader(chunk))));
            }
            targetWriter = new BufferedWriterTarget(new BufferedWriter(new FileWriter(targetFile)));
            final MergeLoggerImpl logger = new MergeLoggerImpl(DEFAULT_MAX_NUMBERS_IN_CHUNK);

            try {
                Merger.merge(chunkReaders, targetWriter, logger);
            } catch (Exception e) {
                throw new IOException(e);
            }

        } finally {
            for (BufferedReaderChunk chunkReader : chunkReaders) {
                chunkReader.getReader().close();
            }
            if (targetWriter != null) {
                targetWriter.getWriter().close();
            }
        }
    }

    private void cleanup(List<File> chunks) {
        System.out.println("Deleting chunks...");

        for (File chunk : chunks) {
            final boolean deleted = chunk.delete();
            if (!deleted) {
                System.err.println("Failed to delete chunk " + chunk.getName());
            }
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Usage: java Extsort.class <source_file> <target_file>");
            return;
        }

        if (args.length != 2) {
            System.err.println("Invalid number of arguments: " + args.length + ", expected: 2");
            System.out.println("Usage: java Extsort.class <source_file> <target_file>");
            return;
        }

        final String sourcePath = args[0];
        final String targetPath = args[1];

        final File sourceFile = new File(sourcePath);
        if (!sourceFile.exists()) {
            System.err.println("No such file: " + sourcePath);
            return;
        }

        final File targetFile = new File(targetPath);

        new Extsort(sourceFile, targetFile).sort();
    }
}