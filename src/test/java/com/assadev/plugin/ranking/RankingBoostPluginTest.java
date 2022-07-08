package com.assadev.plugin.ranking;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

/***
 * 싱글 노드 통합 테스트
 * vm options 에 "-ea -Dtests.security.manager=false" 저장 후 실행
 */
public class RankingBoostPluginTest extends ESSingleNodeTestCase {

    private void initializeSimilarityIndices() throws IOException {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }
        client().admin().indices().prepareCreate("test")
                .setSettings(Settings.builder()
                        .put("index.analysis.analyzer.index_analyzer.tokenizer", "standard")
                        .put("index.analysis.analyzer.index_analyzer.type", "custom")
                        .putList("index.analysis.analyzer.index_analyzer.filter", "lowercase")

                        .put("index.analysis.analyzer.search_analyzer.tokenizer", "standard")
                        .put("index.analysis.analyzer.search_analyzer.type", "custom")
                        .putList("index.analysis.analyzer.search_analyzer.filter", "lowercase")

                )
                .setMapping(jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "text")
                        .field("analyzer", "index_analyzer")
                        .field("search_analyzer", "search_analyzer")
                        .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                        .startObject("field2")
                        .field("type", "text")
                        .field("analyzer", "index_analyzer")
                        .field("search_analyzer", "search_analyzer")
                        .field("term_vector", "with_positions_offsets_payloads")
                        .endObject()
                        .startObject("searchKeyword")
                        .field("type", "text")
                        .field("analyzer", "index_analyzer")
                        .field("search_analyzer", "search_analyzer")
                        .field("term_vector", "with_positions_offsets_payloads")
                        .startObject("fields")
                        .startObject("keyword")
                        .field("type","keyword")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject())
                .execute().actionGet();

    }

    public void testSimilarityBoostPlugin() throws Exception {

        this.initializeSimilarityIndices();

        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("test").id("100000").source("field1", "[특가]팡이제로", "field2", ""));
        request.add(new IndexRequest("test").id("100001").source("field1", "[특가]곰팡이가 많다면? 젤타입!", "field2", ""));
        request.add(new IndexRequest("test").id("100002").source("field1", "[특가]곰팡이스프레이", "field2", ""));
        request.add(new IndexRequest("test").id("100003").source("field1", "[특가]집에 곰팡이 걱정마세요 스프레이 분사 방식", "field2", ""));
        request.add(new IndexRequest("test").id("100004").source("field1", "[특가]곰팡이 제거제 곰팡이젤 곰팡이용"));
        request.add(new IndexRequest("test").id("100005").source("field1", "곰팡이 [특가] 제거제"));
        request.add(new IndexRequest("test").id("100006").source("field1", "[특가]곰팡이용 단백질"));
        request.add(new IndexRequest("test").id("100007").source("field1", "[특가]"));

        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        ActionFuture<BulkResponse> responses = client().bulk(request);
        BulkResponse responses1 = responses.actionGet();

        assertEquals(responses1.hasFailures() == true ? 0l : 1l, 1l);

        Map<String, Object> params = new HashMap<>();
        params.put("field", "field1");
        params.put("weight", 10000.0);
        params.put("tokens", new ArrayList<>(){{
            add("곰팡이");
            add("제거제");
        }});
        ScriptScoreFunctionBuilder score = ScoreFunctionBuilders.scriptFunction(
                new Script(ScriptType.INLINE, "boost_script", "similarity_boost_df", params));

        FunctionScoreQueryBuilder functionScoreQueryBuilder
                = QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("field1", "곰팡이 제거제").operator(Operator.AND), score).scoreMode(FunctionScoreQuery.ScoreMode.SUM).boostMode(CombineFunction.REPLACE);

        SearchResponse searchResponse =
                client().prepareSearch().setQuery(functionScoreQueryBuilder).setExplain(true).execute().actionGet();

        System.out.println("");
        System.out.println("==== search result ====");
        for(SearchHit hit : searchResponse.getHits().getHits()){
            System.out.println("docId: "+ hit.getId() +", score: "+hit.getScore());
        }
        System.out.println("");
    }

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
                        .startObject("title")
                        .field("type", "text")
                        .field("analyzer", "my_analyzer")
                        .endObject()
                        .startObject("categoryIds")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject())
                .setSettings(Settings.builder()
                        .put("index.analysis.analyzer.my_analyzer.type", "custom")
                        .put("index.analysis.analyzer.my_analyzer.filter", "trim")
                        .put("index.analysis.analyzer.my_analyzer.filter", "lowercase")
                        .put("index.analysis.analyzer.my_analyzer.tokenizer", "standard"))
                .execute().actionGet();

        client().prepareIndex("test").setId("1").setSource(jsonBuilder()
                .startObject()
                .field("title", "일자 데님 바지 팬츠")
                .field("categoryIds", "G00003^G00019^G00125^G00737")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder()
                .startObject()
                .field("title", "여성 핀턱 슬랙스")
                .field("categoryIds", "G00003^G00019^G00125")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("3").setSource(jsonBuilder()
                .startObject()
                .field("title", "남성 정장 팬츠")
                .field("categoryIds", "G00006^G00039^G00264")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("4").setSource(jsonBuilder()
                .startObject()
                .field("title", "픽턱 일자 통 바지 데님 자수")
                .field("categoryIds", "G00006^G00039^G00264^G01586")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("5").setSource(jsonBuilder()
                .startObject()
                .field("title", "필승 코리아")
                .field("categoryIds", "G00003^G00021^G00138^G00811")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();
        client().prepareIndex("test").setId("6").setSource(jsonBuilder()
                .startObject()
                .field("title", "한국")
                .field("categoryIds", "G00001^G00011^G00066^G00440")
                .endObject()).setRefreshPolicy(IMMEDIATE).execute().actionGet();

        List<FunctionScoreQueryBuilder.FilterFunctionBuilder> functions = new ArrayList<>();
        functions.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script(ScriptType.INLINE, "boost_script", "category_boost_df", new HashMap<>(){{
            put("field", "categoryIds");
            put("category", "g01586");
            put("weight", 90000.0);
            put("defaultScore", 1000.0);
        }}))));
        functions.add(new FunctionScoreQueryBuilder.FilterFunctionBuilder(ScoreFunctionBuilders.scriptFunction(new Script(ScriptType.INLINE, "boost_script", "category_boost_df", new HashMap<>(){{
            put("field", "categoryIds");
            put("category", "g00737");
            put("weight", -200.0);
            put("defaultScore", 1000.0);
        }}))));


        FunctionScoreQueryBuilder functionScoreQueryBuilder
                = QueryBuilders.functionScoreQuery(matchQuery("title", "일자 데님 바지").operator(Operator.AND), functions.stream().toArray(FunctionScoreQueryBuilder.FilterFunctionBuilder[]::new)).scoreMode(FunctionScoreQuery.ScoreMode.SUM).boostMode(CombineFunction.REPLACE);
        SearchResponse searchResponse =
                client().prepareSearch().setQuery(functionScoreQueryBuilder).execute().actionGet();

        System.out.println("");
        System.out.println("==== search result ====");
        for(SearchHit hit : searchResponse.getHits().getHits()){
            System.out.println("docId: "+ hit.getId() +", score: "+hit.getScore());
        }
        System.out.println("");
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> classPathPlugins = new ArrayList<>();
        classPathPlugins.add(RankingBoostPlugin.class);
        return classPathPlugins;
    }
}
