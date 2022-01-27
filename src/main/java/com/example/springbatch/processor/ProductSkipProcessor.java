package com.example.springbatch.processor;

import com.example.springbatch.model.Product;
import org.springframework.batch.item.ItemProcessor;

public class ProductSkipProcessor implements ItemProcessor<Product, Product> {

    @Override
    public Product process(Product item) throws Exception {
        if (item.getProductId() == 2)
            throw new RuntimeException("Because ID is 2");
        else
            item.setProductDesc(item.getProductDesc().toUpperCase());

        return item;
    }

}