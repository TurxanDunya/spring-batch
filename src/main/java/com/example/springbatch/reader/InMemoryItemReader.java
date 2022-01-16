package com.example.springbatch.reader;

import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

@Component
public class InMemoryItemReader extends AbstractItemStreamItemReader {

    Integer[] intArray = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    List<Integer> integerList = Arrays.asList(intArray);

    int index = 0;

    @Override
    public Object read() {
        Integer nextItem = null;

        if (index < integerList.size()) {
            nextItem = integerList.get(index);
            index++;
        } else {
            index = 0;
        }

        return nextItem;
    }

}
