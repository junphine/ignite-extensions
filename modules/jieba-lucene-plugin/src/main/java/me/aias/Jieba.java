package me.aias;

import me.aias.jieba.JiebaSegmenter;
import me.aias.jieba.SegToken;
import me.aias.jieba.keyword.Keyword;
import me.aias.jieba.keyword.TFIDFAnalyzer;

import java.nio.file.Paths;
import java.util.List;

public final class Jieba {
    private JiebaSegmenter segmenter = new JiebaSegmenter();
    
    private TFIDFAnalyzer tfidfAnalyzer = new TFIDFAnalyzer();

    public Jieba() {
        segmenter.initUserDict(Paths.get("conf","user.dict"));
    }

    public String[] cut(String sentence) {
        List<SegToken> tokens = segmenter.process(sentence, JiebaSegmenter.SegMode.SEARCH);
        String[] words = new String[tokens.size()];
        for (int i = 0; i < tokens.size(); i++) {
            words[i] = tokens.get(i).word;
        }
        return words;
    }
    
    public String[] cutForIndex(String sentence) {
        List<SegToken> tokens = segmenter.process(sentence, JiebaSegmenter.SegMode.INDEX);
        String[] words = new String[tokens.size()];
        for (int i = 0; i < tokens.size(); i++) {
            words[i] = tokens.get(i).word;
        }
        return words;
    }
    
    public String[] keywords(String content, int topN) {
    	List<Keyword> tokens = tfidfAnalyzer.analyze(content, topN);
    	String[] words = new String[tokens.size()];
        for (int i = 0; i < tokens.size(); i++) {
            words[i] = tokens.get(i).getName();
        }
        return words;
    }
}
