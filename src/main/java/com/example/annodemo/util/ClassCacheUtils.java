package com.example.annodemo.util;

import com.example.annodemo.annotation.SqlColumn;
import com.example.annodemo.annotation.SqlIgnore;
import com.example.annodemo.annotation.SqlTable;
import com.google.common.base.CaseFormat;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ClassCacheUtils {
    private static LoadingCache<Class, String> classTableMap = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(20, TimeUnit.MINUTES)
            .build(new CacheLoader<Class, String>() {
                @Override
                public String load(Class clz) {
                    SqlTable annotation = (SqlTable) clz.getAnnotation(SqlTable.class);
                    return (Objects.nonNull(annotation) ? annotation.name() : toUnderscore(clz.getName()));
                }
            });

    private static LoadingCache<Class, LinkedHashMap<String, Field>> classColumnFieldMap = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(20, TimeUnit.MINUTES)
            .build(new CacheLoader<Class, LinkedHashMap<String, Field>>() {
                @Override
                public LinkedHashMap<String, Field> load(Class clz) {
                    // 解析
                    LinkedHashMap<String, Field> columnFieldMap = Maps.newLinkedHashMap();
                    Field[] declaredFields = clz.getDeclaredFields();
                    for (Field declaredField : declaredFields) {
                        // 忽略 SqlIgnore 字段与静态字段
                        if (Objects.nonNull(declaredField.getAnnotation(SqlIgnore.class)) || Modifier.isStatic(declaredField.getModifiers())) {
                            continue;
                        }
                        SqlColumn annotation = declaredField.getAnnotation(SqlColumn.class);
                        declaredField.setAccessible(true);
                        if (Objects.nonNull(annotation)) {
                            columnFieldMap.put(annotation.name(), declaredField);
                        } else {
                            columnFieldMap.put(toUnderscore(declaredField.getName()), declaredField);
                        }
                    }
                    return columnFieldMap;
                }
            });

    /**
     * 驼峰转下划线
     *
     * @param camelCase
     * @return
     */
    private static String toUnderscore(String camelCase) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, camelCase);
    }

    /**
     * 下划线转驼峰
     *
     * @param underscore
     * @return
     */
    private static String toCamelCase(String underscore) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, underscore);
    }

    public static String getTableName(Class clz) {
        try {
            return classTableMap.get(clz);
        } catch (ExecutionException e) {
            log.error("#getTableName# exception, clz: {}", clz);
            return null;
        }
    }

    public static Map<String, Field> getColumnFieldMap(Class clz) {
        try {
            return classColumnFieldMap.get(clz);
        } catch (ExecutionException e) {
            log.error("#getColumnFieldMap# exception, clz: {}", clz);
            return null;
        }
    }
}
