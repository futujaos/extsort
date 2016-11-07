package com.futujaos.extsort.merge;

import java.util.*;

public final class Merger {

    public static <T extends Comparable<T>> void merge(
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