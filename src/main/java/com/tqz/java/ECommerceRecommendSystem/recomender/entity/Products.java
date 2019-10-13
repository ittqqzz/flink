package com.tqz.java.ECommerceRecommendSystem.recomender.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Products {
    Integer productId;
    String name;
    String imageUrl;
    String categories;
    String tags;

    @Override
    public String toString() {
        return "Products{" +
                "productId=" + productId +
                ", name='" + name + '\'' +
                ", imageUrl='" + imageUrl + '\'' +
                ", categories='" + categories + '\'' +
                ", tags='" + tags + '\'' +
                '}';
    }
}
