package com.futujaos.extsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class Generator {
    private static final long N = (1L << 26); // 64M
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {
        final long startTime = System.currentTimeMillis();
        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(new File("source.txt")))) {
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
