package ru.ing;

import ru.ing.util.StringSearch;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

public class IngApplication {
    public static void main(String[] args) {

        String fileName = args[0];

        try {
            Instant startTime = Instant.now();

            new StringSearch().processData(fileName);

            Instant endTime = Instant.now();

            Duration duration = Duration.between(endTime,startTime);
            long seconds = duration.getSeconds();
            int millis = duration.toMillisPart();

            System.out.println("Задание выполнено");
            System.out.println("Время работы: " + seconds + "," + millis + " сек");
        } catch (IOException e) {
            System.err.println("Ошибка: " + e.getMessage());
        }
    }
}