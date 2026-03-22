package me.aias;

import java.util.Arrays;
import java.util.List;

import me.aias.jieba.keyword.Keyword;
import me.aias.jieba.keyword.TFIDFAnalyzer;

public class KeywordTest {
	
    public static void main(String[] args) {
        String sentence = "今天是个好日子";
        Jieba parser = new Jieba();
        for (int i = 0; i < 100; i++) {
            String[] words = parser.cut(sentence);
            System.out.println(Arrays.toString(words));
        }
        
        String content = "孩子上了幼儿园, 亡羊补牢为时不晚安全防拐教育要做好!";
		
		
		int topN = 5;
		TFIDFAnalyzer tfidfAnalyzer = new TFIDFAnalyzer();
		List<Keyword> list = tfidfAnalyzer.analyze(content, topN);
		for (Keyword word : list) {
			System.out.println(word.getName() + ":" + word.getTfidfvalue() + ",");
		}
		// 防拐:0.1992,幼儿园:0.1434,做好:0.1065,教育:0.0946,安全:0.0924


    }
}
