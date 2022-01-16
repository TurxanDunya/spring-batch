package com.example.springbatch.writer;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

// Writes to console
@Component
public class InMemoryItemWriter extends AbstractItemStreamItemWriter {

    @Override
    public void write(List items) throws Exception {
        items.stream().forEach(System.out::println);
        System.out.println("********* writing each chunk **********");
    }

}
