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

public class CategoryScoreFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private final String field;
    private final String category_id;
    private final String operation_type;
    private final int operation_score;
    private final int default_score;

    private final static String FIELD_NAME = "field";
    private final static String CATEGORYID_NAME = "category_id";
    private final static String OPERATION_MODE_NAME = "operation_mode";
    private final static String OPERATION_SCORE_NAME = "operation_score";
    private final static String DEFAULT_SCORE_NAME = "default_score";

    public CategoryScoreFactory(Map<String, Object> params, SearchLookup lookup) {
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
        category_id = params.get(CATEGORYID_NAME).toString();
        operation_type = (String) params.get(OPERATION_MODE_NAME);
        if(!operation_type.equals("-") && !operation_type.equals("+") && !operation_type.equals("except"))
        {
            throw new IllegalArgumentException(
                    "Missing operation_type ["+operation_type+"]");
        }
        operation_score = (int) params.get(OPERATION_SCORE_NAME);
        default_score = (int) params.get(DEFAULT_SCORE_NAME);
    }

    @Override
    public boolean needs_score() {
        return false;
    }

    @Override
    public ScoreScript newInstance(DocReader reader) throws IOException {
        return null;
    }

    @Override
    public ScoreScript newInstance(LeafReaderContext context) throws IOException {
        PostingsEnum postings = context.reader().postings(new Term(field, category_id), PostingsEnum.ALL);

        if (postings == null) {
            return new ScoreScript(params, lookup, context) {
                @Override
                public double execute(ExplanationHolder explanationHolder) {
                    return default_score;
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
                    return default_score; //매칭이 되지 않아도 기본 스코어 적용
                }
                try {
                    int freq = postings.freq();
                    if(freq <= 0)
                    {
                        return default_score;
                    }

                    float sum_score = 0.0f;
                    for (int i = 0; i < freq; i++) { // field에 token이 매칭 된 숫자 만큼 loop
                        if(operation_type.equals("-")) {
                            sum_score += default_score - operation_score;
                        }
                        else if(operation_type.equals("+")){
                            sum_score += operation_score + default_score;
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