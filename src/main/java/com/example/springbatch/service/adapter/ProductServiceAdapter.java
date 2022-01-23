package com.example.springbatch.service.adapter;

import com.example.springbatch.model.Product;
import com.example.springbatch.service.ProductService;
import lombok.Data;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Why we created this class?
 * Because, when spring batch works, every time it requests for getProducts and accepts response.
 * So, spring batch works infinitely. To stop that, we are checking whether the next object is null
 * or not. If it is null, then batch will stop the work.
 * Instead of injecting real service class to batch configuration, we will inject adapter.
 */
@Data
@Component
public class ProductServiceAdapter implements InitializingBean {

    @Autowired
    private ProductService productService;

    private List<Product> products;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.products = productService.getProducts();
    }

    public Product nextProduct() {
        if (products.size() > 1)
            return products.remove(0);

        return null;
    }

}
