package com.assadev.plugin.ranking.boost;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/***
 * 싱글 노드 통합 테스트
 * vm options 에 "-ea -Dtests.security.manager=false" 저장 후 실행
 */
public class CategoryRankingTest extends ESSingleNodeTestCase {
    /***
     * 카테고리 부스트 플러그인 통합 테스트
     * @throws Exception
     */
    public void testCategoryBoostPlugin() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test")
                .setMapping(jsonBuilder().startObject()
                        .startObject("type1")
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "text")
                        .endObject()
                        .startObject("categoryIds")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .setSettings(Settings.builder()
                        .put("index.analysis.analyzer.my_stop_analyzer.tokenizer", "standard")
                        .put("index.analysis.analyzer.my_stop_analyzer.filter", "trim")
                        .put("index.analysis.analyzer.my_stop_analyzer.filter", "lowercase")
                ).execute().actionGet();

        client().prepareIndex("test").setId("1").setSource("field1", "the quick brown fox jumped over the lazy dog", "categoryIds", "100000/100001/100002").setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("2").setSource("field1", "the quick brown fox jumped over the lazy dog", "categoryIds", "200000/200001/200002").setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("3").setSource("field1", "hello world!", "categoryIds", "200000/200001/200002").setRefreshPolicy(IMMEDIATE).execute().actionGet();

        Map<String, Object> params = new HashMap<>();
        params.put("field", "categoryIds");
        params.put("category_id", "200002");
        params.put("operation_mode", "+");
        params.put("operation_score", 5);
        params.put("default_score", 20);

        ScriptScoreFunctionBuilder score = ScoreFunctionBuilders.scriptFunction(
                new Script(ScriptType.INLINE, "boost_script", "category_boost_df", params));

        FunctionScoreQueryBuilder functionScoreQueryBuilder
                = QueryBuilders.functionScoreQuery(matchQuery("field1", "quick brown"), score).scoreMode(FunctionScoreQuery.ScoreMode.SUM).boostMode(CombineFunction.REPLACE);
        MatchQueryBuilder matchQueryBuilder = matchQuery("field1", "quick");

        SearchResponse searchResponse =
                client().prepareSearch().setQuery(functionScoreQueryBuilder).execute().actionGet();

        assertHitCount(searchResponse, 2);
        assertFirstHit(searchResponse, hasId("2"));

    }
}
