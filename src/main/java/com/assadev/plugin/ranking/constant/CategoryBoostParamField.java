package com.assadev.plugin.ranking.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CategoryBoostParamField {
    FIELD_NAME("field"),
    CATEGORY_NAME("category"),             // 카테고리 정보
    WEIGHT_NAME("weight"),     // 부스팅 점수
    DEFAULT_SCORE_NAME("defaultScore");         // 기본 점수

    private final String fieldName;
}
