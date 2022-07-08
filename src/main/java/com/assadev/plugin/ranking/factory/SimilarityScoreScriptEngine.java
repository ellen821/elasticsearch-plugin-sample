package com.assadev.plugin.ranking.factory;

import com.assadev.plugin.ranking.constant.SimilarityBoostParamField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ExplainableScoreScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SimilarityScoreScriptEngine extends ScoreScript implements ExplainableScoreScript {

    private final Map<String, Object> params;
    private final DocReader reader;

    private final String field;
    private final List<String> tokens;
    private final double weight;

    public SimilarityScoreScriptEngine(Map<String, Object> params, SearchLookup lookup, DocReader reader) {
        super(params, lookup, reader);

        this.params = params;
        this.reader = reader;

        if (this.params.containsKey(SimilarityBoostParamField.FIELD_NAME.getFieldName()) == false) {
            throw new IllegalArgumentException("Missing parameter [" + SimilarityBoostParamField.FIELD_NAME.getFieldName() + "]");
        }
        if (this.params.containsKey(SimilarityBoostParamField.TOKENS_NAME.getFieldName()) == false) {
            throw new IllegalArgumentException("Missing parameter [" + SimilarityBoostParamField.TOKENS_NAME.getFieldName() + "]");
        }
        if (this.params.containsKey(SimilarityBoostParamField.WEIGHT_NAME.getFieldName()) == false) {
            throw new IllegalArgumentException("Missing parameter [" + SimilarityBoostParamField.WEIGHT_NAME.getFieldName() + "]");
        }
        this.field = params.get(SimilarityBoostParamField.FIELD_NAME.getFieldName()).toString();
        this.tokens = (List<String>) params.get(SimilarityBoostParamField.TOKENS_NAME.getFieldName());
        this.weight = (double) params.get(SimilarityBoostParamField.WEIGHT_NAME.getFieldName());
    }

    @Override
    public Explanation explain(Explanation explanation) throws IOException {
        return Explanation.match(0, "field_boost", new Explanation[0]);
    }

    @Override
    public double execute(ExplanationHolder explanation) {
        double sum_score = 0.0d; //필드 별 점수
        int position = -1;
        for (String token : this.tokens) {

            /***
             * 유사도 점수 매칭
             */
            double score = this.match(token, _getDocId());

            if (score > 0.0) { ///확장어 중에 매칭 된 것이 존재 하면 뒤 확장어 매칭을 더 할 필요 없다.
                sum_score += score;
                break;
            }
        }
        return sum_score;
    }

    private double match(String token, int docId){
        double score = 0.0d;
        PostingsEnum postings = null;
        DocValuesDocReader dvReader = ((DocValuesDocReader) reader);
        try {
            postings = dvReader.getLeafReaderContext().reader().postings(new Term(field, token), PostingsEnum.ALL);
            if (postings != null) {
                if (postings.docID() < docId) {
                    postings.advance(docId);
                }
                if (postings.docID() == docId) {
                    score = weight;
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
        return score;
    }
}
