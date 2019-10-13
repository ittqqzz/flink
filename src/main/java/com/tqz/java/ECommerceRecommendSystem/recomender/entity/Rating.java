package com.tqz.java.ECommerceRecommendSystem.recomender.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Rating {
    private Integer userId;
    private String productId;
    private Double score;
    private Integer timestamp;
}
