package com.tqz.java.ECommerceRecommendSystem.recomender.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Rating {
    public Integer userId;
    public String productId;
    public Double score;
    public Integer timestamp;
}
