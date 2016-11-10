package com.futujaos.extsort.merge;

import java.io.*;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.futujaos.extsort.merge.impl.BufferedReaderChunk;
import com.futujaos.extsort.merge.impl.BufferedWriterTarget;
import com.futujaos.extsort.merge.impl.MergeLoggerImpl;

public final class Merger {
    private static final Logger logger = LogManager.getLogger();
    private final List<File> chunks;
    private final File targetFile;
    private final int logStep;

    public Merger(List<File> chunks, File targetFile, int logStep) {
        this.chunks = chunks;
        this.targetFile = targetFile;
        this.logStep = logStep;
    }

    public void merge() throws IOException {
        logger.info("Joining " + chunks.size() + " chunks to target file...");

        final List<BufferedReaderChunk> chunkReaders = new ArrayList<>(chunks.size());
        BufferedWriterTarget targetWriter = null;
        try {
            for (File chunk : chunks) {
                chunkReaders.add(new BufferedReaderChunk(new BufferedReader(new FileReader(chunk))));
            }
            targetWriter = new BufferedWriterTarget(new BufferedWriter(new FileWriter(targetFile)));
            final MergeLoggerImpl logger = new MergeLoggerImpl(logStep);

            try {
                runMerge(chunkReaders, targetWriter, logger);
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

        logger.info("Deleting chunks...");

        for (File chunk : chunks) {
            final boolean deleted = chunk.delete();
            if (!deleted) {
                logger.error("Failed to delete chunk " + chunk.getName());
            }
        }
    }

    private static <T extends Comparable<T>> void runMerge(
            Collection<? extends MergeChunk<T>> chunks,
            MergeTarget<T> target,
            MergeLogger logger
    ) throws Exception {

        Objects.requireNonNull(chunks);
        Objects.requireNonNull(target);

        final Map<Integer, MergeChunk<T>> index2chunk = new HashMap<>(chunks.size());
        int chunkIndex = 0;
        for (MergeChunk<T> chunk : chunks) {
            index2chunk.put(chunkIndex++, chunk);
        }

        final PriorityQueue<ChunkElement<T>> queue = new PriorityQueue<>(chunks.size());

        for (Map.Entry<Integer, MergeChunk<T>> entry : index2chunk.entrySet()) {
            final T element = entry.getValue().readElement();
            final int fromChunk = entry.getKey();
            queue.add(new ChunkElement<>(element, fromChunk));
        }

        int elementsMerged = 0;

        while (true) {
            final ChunkElement<T> chunkElement = queue.poll();
            target.writeElement(chunkElement.element);
            logger.logIfNeeded(++elementsMerged);

            final MergeChunk<T> chunk = index2chunk.get(chunkElement.fromChunk);
            if (chunk == null) {
                continue;
            }
            final T nextElement = chunk.readElement();
            if (nextElement == null) {
                index2chunk.remove(chunkElement.fromChunk);
                if (index2chunk.isEmpty()) {
                    break;
                }
                final int firstChunkIndex = index2chunk.keySet().iterator().next();
                final MergeChunk<T> firstChunk = index2chunk.get(firstChunkIndex);
                final T firstChunkElement = firstChunk.readElement();
                if (firstChunkElement == null) {
                    break;
                }
                queue.add(new ChunkElement<>(firstChunkElement, firstChunkIndex));
            } else {
                queue.add(new ChunkElement<>(nextElement, chunkElement.fromChunk));
            }
        }

        while (!queue.isEmpty()) {
            final T element = queue.poll().element;
            target.writeElement(element);
            logger.logIfNeeded(++elementsMerged);
        }
    }

    private static final class ChunkElement<V extends Comparable<V>> implements Comparable<ChunkElement<V>> {
        private final V element;
        private final int fromChunk;

        public ChunkElement(V element, int fromChunk) {
            this.element = element;
            this.fromChunk = fromChunk;
        }

        @Override
        public int compareTo(ChunkElement<V> o) {
            return element.compareTo(o.element);
        }
    }
}