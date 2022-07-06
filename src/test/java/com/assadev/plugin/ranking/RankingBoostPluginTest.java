package com.assadev.plugin.ranking;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.*;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/***
 * 싱글 노드 통합 테스트
 * vm options 에 "-ea -Dtests.security.manager=false" 저장 후 실행
 */
public class RankingBoostPluginTest extends ESSingleNodeTestCase {

    public void testCategoryBoostPlugin() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

//        URL resource = getClass().getResource(Paths.get(File.separatorChar + "settings.json").toString());

        client().admin().indices().prepareCreate("test")
                .setMapping(jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "text")
                        .field("analyzer", "my_analyzer")
                        .endObject()
                        .startObject("categoryIds")
                        .field("type", "keyword")
                        .endObject()
                        .endObject()
                        .endObject())
//                .setSettings(jsonBuilder()
//                        .startObject()
//                        .startObject("analysis")
//                        .startObject("analyzer")
//                        .startObject("my_analyzer")
//                        .field("type", "custom")
//                        .array("filter", "lowercase")
//                        .field("tokenizer", "standard")
//                        .endObject()
//                        .endObject()
//                        .startObject("tokenizer")
//                        .startObject("my_analyzer")
//                        .field("type", "pattern")
//                        .field("pattern", "@")
//                        .endObject()
//                        .endObject()
//                        .endObject()
//                        .endObject()
//                )
                .setSettings(Settings.builder()
                        .put("index.analysis.analyzer.my_analyzer.type", "custom")
                        .put("index.analysis.analyzer.my_analyzer.filter", "trim")
                        .put("index.analysis.analyzer.my_analyzer.filter", "lowercase")
                        .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard"))
//                .setSettings(copyToStringFromClasspath("/settings.json"), XContentType.JSON)
                .execute().actionGet();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder()
                .startObject()
                .field("field1", "the quick brown fox jumped over the lazy dog")
                .array("categoryIds", "G00001G00011G00066G00440", "G00001G00011G00066", "G00001", "G00001G00011")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder()
                .startObject()
                .field("field1", "the quick brown fox jumped over the lazy dog")
                .array("categoryIds", "G00002", "G00002G00016G00099", "G00002G00016", "G00002G00016G00099G00594")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("3").setSource(jsonBuilder()
                .startObject()
                .field("field1", "hello world! quick brown")
                .array("categoryIds", "G00007", "G00007G00041G00279", "G00007G00041", "G00007G00041G00279G01703")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();

        List<Map<String, Object>> categoryList = new ArrayList<>();
        categoryList.add(new HashMap<>(){{
            put("category", "G00002G00016G00099G00594");
            put("weight", 90000);
        }});
        categoryList.add(new HashMap<>(){{
            put("category", "G00001G00011G00066");
            put("weight", -200);
        }});

        Map<String, Object> params = new HashMap<>();
        params.put("field", "categoryIds");
        params.put("categoryList", categoryList);
        params.put("defaultScore", 1000);

        ScriptScoreFunctionBuilder score = ScoreFunctionBuilders.scriptFunction(
                new Script(ScriptType.INLINE, "boost_script", "category_boost_df", params));

        FunctionScoreQueryBuilder functionScoreQueryBuilder
                = QueryBuilders.functionScoreQuery(matchQuery("field1", "quick brown"), score).scoreMode(FunctionScoreQuery.ScoreMode.SUM).boostMode(CombineFunction.REPLACE);
        MatchQueryBuilder matchQueryBuilder = matchQuery("field1", "quick");

        SearchResponse searchResponse =
                client().prepareSearch().setQuery(functionScoreQueryBuilder).execute().actionGet();

        System.out.println("");
        System.out.println("==== search result ====");
        for(SearchHit hit : searchResponse.getHits().getHits()){
            System.out.println("docId: "+ hit.getId() +", score: "+hit.getScore());
        }
        System.out.println("");

//        assertHitCount(searchResponse, 2);
//        assertFirstHit(searchResponse, hasId("2"));

    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classPathPlugins = new ArrayList<>();
        classPathPlugins.add(RankingBoostPlugin.class);
        return classPathPlugins;
    }
}
