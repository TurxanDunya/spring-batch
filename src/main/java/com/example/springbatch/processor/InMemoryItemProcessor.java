package com.example.springbatch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class InMemoryItemProcessor implements ItemProcessor<Integer, Integer> {

    @Override
    public Integer process(Integer item) throws Exception {
        return Integer.sum(10, item);
    }

}