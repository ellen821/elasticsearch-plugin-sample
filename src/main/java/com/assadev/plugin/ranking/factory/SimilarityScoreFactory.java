package com.assadev.plugin.ranking.factory;

import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

public class SimilarityScoreFactory implements ScoreScript.LeafFactory {

    private final Map<String, Object> params;
    private final SearchLookup lookup;
    public SimilarityScoreFactory(Map<String, Object> params, SearchLookup lookup) {
        this.params = params;
        this.lookup = lookup;
    }

    @Override
    public boolean needs_score() {
        return false;
    }

    @Override
    public ScoreScript newInstance(DocReader reader) throws IOException {
        return new SimilarityScoreScriptEngine(params,lookup,reader);
    }
}
