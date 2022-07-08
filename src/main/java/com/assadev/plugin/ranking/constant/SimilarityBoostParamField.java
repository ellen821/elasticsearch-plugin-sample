package com.assadev.plugin.ranking.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum SimilarityBoostParamField {
    FIELD_NAME("field"),
    TOKENS_NAME("tokens"),             // 토큰 정보
    WEIGHT_NAME("weight");              // 부스팅 점수

    private final String fieldName;
}
