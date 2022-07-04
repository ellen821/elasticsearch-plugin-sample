package com.assadev.plugin.ranking.boost;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

public class CategoryBoostFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private final String field;
    private final String categoryId;
    private final String operationType;
    private final int operationScore;
    private final int defaultScore;

    private final static String FIELD_NAME = "field";
    private final static String CATEGORYID_NAME = "categoryId";
    private final static String OPERATION_MODE_NAME = "operationMode";
    private final static String OPERATION_SCORE_NAME = "operationScore";
    private final static String DEFAULT_SCORE_NAME = "defaultScore";

    public CategoryBoostFactory(Map<String, Object> params, SearchLookup lookup) {
        if (params.containsKey(FIELD_NAME) == false) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+FIELD_NAME+"]");
        }
        if (params.containsKey(CATEGORYID_NAME) == false) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CATEGORYID_NAME+"]");
        }
        if (params.containsKey(OPERATION_MODE_NAME) == false) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+OPERATION_MODE_NAME+"]");
        }
        if (params.containsKey(OPERATION_SCORE_NAME) == false) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+OPERATION_SCORE_NAME+"]");
        }
        if (params.containsKey(DEFAULT_SCORE_NAME) == false) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+DEFAULT_SCORE_NAME+"]");
        }
        this.params = params;
        this.lookup = lookup;
        field = params.get(FIELD_NAME).toString();
        categoryId = params.get(CATEGORYID_NAME).toString();
        operationType = (String) params.get(OPERATION_MODE_NAME);
        if(!operationType.equals("-") && !operationType.equals("+") && !operationType.equals("except"))
        {
            throw new IllegalArgumentException(
                    "Missing operation_type ["+ operationType +"]");
        }
        operationScore = (int) params.get(OPERATION_SCORE_NAME);
        defaultScore = (int) params.get(DEFAULT_SCORE_NAME);
    }

    @Override
    public boolean needs_score() {
        return false;
    }

//    @Override
//    public ScoreScript newInstance(DocReader reader) throws IOException {
//        return null;
//    }
    @Override
    public ScoreScript newInstance(DocReader reader) throws IOException {
//    public ScoreScript newInstance(LeafReaderContext context) throws IOException {

        reader.

        PostingsEnum postings = context.reader().postings(new Term(field, categoryId), PostingsEnum.ALL);

        if (postings == null) {
            return new ScoreScript(params, lookup, context) {
                @Override
                public double execute(ExplanationHolder explanationHolder) {
                    return defaultScore;
                }
            };
        }
        return new ScoreScript(params, lookup, context) {
            int currentDocid = -1;

            @Override
            public void setDocument(int docid) {
                if (postings.docID() < docid) {
                    try {
                        postings.advance(docid);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                currentDocid = docid;
            }

            @Override
            public double execute(ExplanationHolder explanationHolder) {
                if (postings.docID() != currentDocid) {
                    //return 0.0d; //no match pass
                    return defaultScore; //매칭이 되지 않아도 기본 스코어 적용
                }
                try {
                    int freq = postings.freq();
                    if(freq <= 0)
                    {
                        return defaultScore;
                    }

                    float sum_score = 0.0f;
                    for (int i = 0; i < freq; i++) { // field에 token이 매칭 된 숫자 만큼 loop
                        if(operationType.equals("-")) {
                            sum_score += defaultScore - operationScore;
                        }
                        else if(operationType.equals("+")){
                            sum_score += operationScore + defaultScore;
                        }
                        postings.nextPosition(); //다음 토큰 매칭 구간으로 이동
                    }

                    return sum_score;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }
}