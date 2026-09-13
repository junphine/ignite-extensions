package org.apache.ignite.springdata.repository.support;

import org.springframework.data.annotation.Id;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class IdExtractor {

    // 类 -> 主键字段
    private static final Map<Class<?>, Field> ID_FIELD_CACHE = new ConcurrentHashMap<>();

    // 找不到时的占位，避免每次都重新遍历
    private static final Field NOT_FOUND;
    static {
        try {
            NOT_FOUND = IdExtractor.class.getDeclaredField("NOT_FOUND");
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static boolean isEnable(){
        return true;
    }

    public static Object getId(Object entity) {
        Field field = ID_FIELD_CACHE.computeIfAbsent(entity.getClass(), IdExtractor::resolveIdField);
        if (field == NOT_FOUND) {
            throw new IllegalStateException("无法从 " + entity.getClass() + " 提取主键");
        }
        try {
            return field.get(entity);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("读取主键失败: " + entity.getClass(), e);
        }
    }

    private static Field resolveIdField(Class<?> clazz) {
        Class<?> cur = clazz;
        while (cur != null && cur != Object.class) {
            for (Field f : cur.getDeclaredFields()) {
                if (isIdField(f)) {
                    f.setAccessible(true);
                    return f;
                }
            }
            cur = cur.getSuperclass();
        }
        return NOT_FOUND;
    }

    private static boolean isIdField(Field f) {
        if (f.isAnnotationPresent(Id.class)) {
            return true;
        }
        // 兼容 jakarta.persistence.Id / javax.persistence.Id，避免 classpath 缺失
        for (java.lang.annotation.Annotation a : f.getAnnotations()) {
            String name = a.annotationType().getName();
            if ("jakarta.persistence.Id".equals(name) || "javax.persistence.Id".equals(name)) {
                return true;
            }
        }
        // 兜底：字段名约定
        return "id".equals(f.getName());
    }

    public static Class<?> getIdType(Class<?> entityClass) {
        Field field = ID_FIELD_CACHE.computeIfAbsent(entityClass, IdExtractor::resolveIdField);
        if (field == NOT_FOUND) {
            throw new IllegalStateException("无法从 " + entityClass + " 提取主键");
        }
        return field.getType();
    }
}
