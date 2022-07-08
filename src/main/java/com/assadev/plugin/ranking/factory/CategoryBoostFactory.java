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
import java.util.Map;

public class CategoryBoostFactory implements ScoreScript.LeafFactory {
    private final Map<String, Object> params;
    private final SearchLookup lookup;
    private final String field;
    private final String category;
    private final double weight;
    private final double defaultScore;

    public CategoryBoostFactory(Map<String, Object> params, SearchLookup lookup) {
        if (!params.containsKey(CategoryBoostParamField.FIELD_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.FIELD_NAME.getFieldName()+"]");
        }
        if (!params.containsKey(CategoryBoostParamField.CATEGORY_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.CATEGORY_NAME.getFieldName()+"]");
        }
        if (!params.containsKey(CategoryBoostParamField.WEIGHT_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.WEIGHT_NAME.getFieldName()+"]");
        }
        if (!params.containsKey(CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName())) {
            throw new IllegalArgumentException(
                    "Missing parameter ["+CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName()+"]");
        }
        this.params = params;
        this.lookup = lookup;
        field = params.get(CategoryBoostParamField.FIELD_NAME.getFieldName()).toString();
        category = (String) params.get(CategoryBoostParamField.CATEGORY_NAME.getFieldName());
        weight = (double) params.get(CategoryBoostParamField.WEIGHT_NAME.getFieldName());
        defaultScore = (double) params.get(CategoryBoostParamField.DEFAULT_SCORE_NAME.getFieldName());
    }



    @Override
    public boolean needs_score() {
        return false;
    }

    @Override
    public ScoreScript newInstance(DocReader reader) throws IOException {
        DocValuesDocReader dvReader = ((DocValuesDocReader) reader);
        PostingsEnum postings = dvReader.getLeafReaderContext().reader().postings(new Term(field, category), PostingsEnum.ALL);

//        System.out.println("");

        if(postings == null){
            return new ScoreScript(params, lookup, reader) {
                @Override
                public double execute(ExplanationHolder explanationHolder) {
//                    System.out.println("no match category -> " + category + ", weight -> " + weight);
                    return defaultScore;
                }
            };
        }

        return new ScoreScript(params, lookup, reader) {
            int currentDocid = -1;

            @Override
            public void setDocument(int docid) {
                /*
                 * advance has undefined behavior calling with
                 * a docid <= its current docid
                 */
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
                // 매칭이 되지 않아도 기본 스코어 적용
                if (postings.docID() != currentDocid) {
//                    System.out.println("3");
                    return defaultScore;
                }

//                System.out.println("select category -> " + category + ", weight -> " + weight);

                return defaultScore + weight;
            }
        };
    }
}
