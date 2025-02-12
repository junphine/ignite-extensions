package de.bwaldvogel.examples;

import java.util.LinkedHashMap;

public class DocumentUtilTest {
	
	public static void main(String[] args) {
        // 创建一个LinkedHashMap
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("_id", null);
        map.put("1", "One");
        map.put("2", "Two");
        map.put("3", "Three");

        // 打印原始映射
        System.out.println("Original map: " + map);

        
        // 将元素插入到LinkedHashMap的开头
        map.put("_id", "123");

        // 打印修改后的映射
        System.out.println("Modified map: " + map);
    }

}
