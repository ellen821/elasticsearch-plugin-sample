package com.assadev.plugin.ranking.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public enum CategoryBoostParamField {
    TARGET_FIELD_NAME("field"),
    CATEGORY_LIST_NAME("categoryList"),             // 카테고리 정보
    DEFAULT_SCORE_NAME("defaultScore");         // 기본 점수

    private final String fieldName;
}
