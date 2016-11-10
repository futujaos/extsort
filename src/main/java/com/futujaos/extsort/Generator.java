package com.futujaos.extsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public final class Generator {
    private static final long N = (1L << 26); // 64M
    private static final Random random = new Random();
    private final File targetFile;

    public Generator(File targetFile) {
        this.targetFile = targetFile;
    }

    public static void main(String[] args) throws IOException {
        final File file = new File("source.txt");
        new Generator(file).generate();
    }

    public void generate() throws IOException {
        final long startTime = System.currentTimeMillis();
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(targetFile))) {
            for (long i = 0; i < N; i++) {
                if (i != 0 && i % (2 << 22) == 0) {
                    System.out.println(i + " numbers generated");
                }
                final int number = random.nextInt();
                writer.write(String.valueOf(number));
                writer.write("\n");
            }
        }
        System.out.println((System.currentTimeMillis() - startTime) + " millis elapsed");
    }
}
