package com.assadev.plugin.ranking.factory;

import com.assadev.plugin.ranking.constant.CategoryBoostParamField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class CategoryBoostFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private final String field;
    private final List<Map<String, Object>> categoryList;
    private final int defaultScore;

    public CategoryBoostFactory(Map<String, Object> params, SearchLookup lookup) {
        if (!params.containsKey(CategoryBoostParamField.TARGET_FIELD_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.TARGET_FIELD_NAME.getFieldName()+"]");
        }
        if (!params.containsKey(CategoryBoostParamField.CATEGORY_LIST_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.CATEGORY_LIST_NAME.getFieldName()+"]");
        }
        if (!params.containsKey(CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName()+"]");
        }
        this.params = params;
        this.lookup = lookup;
        field = params.get(CategoryBoostParamField.TARGET_FIELD_NAME.getFieldName()).toString();
        categoryList = (List<Map<String, Object>>) params.get(CategoryBoostParamField.CATEGORY_LIST_NAME.getFieldName());
        defaultScore = (int) params.get(CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName());
    }



    @Override
    public boolean needs_score() {
        return false;
    }

    @Override
    public ScoreScript newInstance(DocReader reader) throws IOException {
        DocValuesDocReader dvReader = ((DocValuesDocReader) reader);

        PostingsEnum prevPostings = null;
        PostingsEnum postings = null;
        HashSet<Integer> weightSet = new HashSet<>();
        for(Map<String, Object> entry : categoryList){
            postings = dvReader.getLeafReaderContext().reader().postings(new Term(field, (String) entry.get("category")), PostingsEnum.ALL);

            if (postings != null) {
                prevPostings = postings;
                weightSet.add((int) entry.get("weight"));
            }
        }

        if(weightSet.size() == 0){
            return new ScoreScript(params, lookup, reader) {
                @Override
                public double execute(ExplanationHolder explanationHolder) {
                    return defaultScore;
                }
            };
        }

        PostingsEnum finalPostings = prevPostings;
        int weight = weightSet.stream().max(Comparator.naturalOrder()).get();

        return new ScoreScript(params, lookup, reader) {
            int currentDocid = -1;

            @Override
            public void setDocument(int docid) {
                /*
                 * advance has undefined behavior calling with
                 * a docid <= its current docid
                 */
                if (finalPostings.docID() < docid) {
                    try {
                        finalPostings.advance(docid);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                currentDocid = docid;
            }

            @Override
            public double execute(ExplanationHolder explanationHolder) {
                // 매칭이 되지 않아도 기본 스코어 적용
                if (finalPostings.docID() != currentDocid) {
                    return defaultScore;
                }

                int score = weight;
                if( score < 0 ){
                    score += defaultScore;
                    return score <= 0 ? 0.0 : score;
                }

                return score;
            }
        };
    }
}
