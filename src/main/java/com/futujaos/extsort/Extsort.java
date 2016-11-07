package com.futujaos.extsort;

import com.futujaos.extsort.merge.Merger;
import com.futujaos.extsort.merge.impl.BufferedReaderChunk;
import com.futujaos.extsort.merge.impl.BufferedWriterTarget;
import com.futujaos.extsort.merge.impl.MergeLoggerImpl;

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
        final List<File> chunks = split();
        join(chunks);
        cleanup(chunks);
        System.out.println("Done!");
    }

    private List<File> split() throws IOException {
        System.out.println("Splitting source file to chunks...");

        final String chunkPrefix = "chunk_" + System.currentTimeMillis() + '_';
        final List<File> chunks = new ArrayList<>();

        try (final BufferedReader sourceReader = new BufferedReader(new FileReader(sourceFile))) {
            chunks.add(new File(chunkPrefix + "0"));
            int[] array = new int[DEFAULT_MAX_NUMBERS_IN_CHUNK];

            int i = 0;
            String line = sourceReader.readLine();
            while (line != null) {
                if (i == DEFAULT_MAX_NUMBERS_IN_CHUNK) {
                    System.out.println("Writing " + array.length + " sorted numbers to chunk #" + chunks.size());
                    Arrays.sort(array);
                    final File currentChunk = chunks.get(chunks.size() - 1);
                    writeToChunk(currentChunk, array);
                    chunks.add(new File(chunkPrefix + chunks.size()));
                    i = 0;
                }

                array[i] = Integer.valueOf(line);

                i++;
                line = sourceReader.readLine();
            }

            if (array.length != 0) {
                System.out.println("Writing " + array.length + " sorted numbers to chunk #" + chunks.size());
                Arrays.sort(array);
                final File currentChunk = chunks.get(chunks.size() - 1);
                writeToChunk(currentChunk, array);
            }
        }

        return chunks;
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

    private void writeToChunk(File chunk, int[] array) throws IOException {
        try (final BufferedWriter chunkWriter = new BufferedWriter(new FileWriter(chunk))) {
            for (int n : array) {
                chunkWriter.write(String.valueOf(n));
                chunkWriter.write("\n");
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