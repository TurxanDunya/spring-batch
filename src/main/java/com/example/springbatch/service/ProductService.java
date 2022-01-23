package com.example.springbatch.service;

import com.example.springbatch.model.Product;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Service
public class ProductService {

    private static final String URL = "http://localhost:8080/products";

    public List<Product> getProducts() {
        RestTemplate restTemplate = new RestTemplate();
        Product[] products = restTemplate.getForObject(URL, Product[].class);

        if (Objects.isNull(products))
            return Collections.emptyList();

        return new ArrayList<>(Arrays.asList(products));
    }

}
