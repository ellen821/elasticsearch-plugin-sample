package com.assadev.plugin.ranking;

import com.assadev.plugin.ranking.factory.CategoryBoostFactory;
import com.assadev.plugin.ranking.factory.SimilarityScoreFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class RankingBoostPlugin extends Plugin implements ScriptPlugin {

    @Override
    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
        return new BoostScoreScriptEngine();
    }

    private static class BoostScoreScriptEngine implements ScriptEngine {
        // 카테고리 부스트
        private final String CATEGORY_BOOST_SOURCE_NAME = "category_boost_df";
        // 시소러스 유사도
        private final String _SIMILARITY_BOOST_SOURCE_VALUE = "similarity_boost_df";

        @Override
        public String getType() {
            return "boost_script";
        }

        @Override
        public <FactoryType> FactoryType compile(String scriptName, String scriptSource, ScriptContext<FactoryType> context, Map<String, String> params) {
            if (!context.equals(ScoreScript.CONTEXT)) {
                throw new IllegalArgumentException(getType()
                        + " scripts cannot be used for context ["
                        + context.name + "]");
            }

            if (CATEGORY_BOOST_SOURCE_NAME.equals(scriptSource)) {
                ScoreScript.Factory factory = CategoryBoostFactory::new;
                return context.factoryClazz.cast(factory);
            }else if(_SIMILARITY_BOOST_SOURCE_VALUE.equals(scriptSource))
            {
                ScoreScript.Factory factory = SimilarityScoreFactory::new;
                return context.factoryClazz.cast(factory);
            }

            throw new IllegalArgumentException("Unknown script name " + scriptSource);
        }

        @Override
        public Set<ScriptContext<?>> getSupportedContexts() {
            return Collections.singleton(ScoreScript.CONTEXT);
        }
    }
}
