package com.example.annodemo.util;

import com.example.annodemo.annotation.SqlColumn;
import com.example.annodemo.annotation.SqlIgnore;
import com.example.annodemo.enums.SqlRelEnum;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class SqlUtils {
    private static final String COLUMN_SEPARATOR = "\\x01";
    private static String COMMA = ",";
    private static String BETWEEN = " BETWEEN ";
    private static String AND = " AND ";
    private static String EQUAL = " = ";
    private static String IN = " in ";
    private static String LEFT_PARENTHESIS = "(";
    private static String RIGHT_PARENTHESIS = ")";
    private static String SINGLE_QUOTAS = "'";
    private static String VAR_BTAG = "#{";
    private static String VAR_ETAG = "}";
    private static String NEW_LINE = "\n";
    private static String COUNT_DISTINCT = " count(distinct %s) ";
    private static String SPACE = " ";

    public static String selectSQL(Class tabClass, Object wp) {
        return selectSQL(tabClass, null, wp);
    }

    public static String selectSQL(Class tabClass, List<String> cols, Object wp) {
        return new SQL()
                .SELECT(CollectionUtils.isEmpty(cols)
                        ? StringUtils.join(Objects.requireNonNull(ClassCacheUtils.getColumnFieldMap(tabClass)).keySet(), COMMA)
                        : StringUtils.join(cols, COMMA))
                .FROM(ClassCacheUtils.getTableName(tabClass))
                .WHERE(parseWhereParams(wp))
                .toString();
    }

    public static String countSQL(Class tabClass, String col, Object wp) {
        return new SQL()
                .SELECT(String.format(COUNT_DISTINCT, col))
                .FROM(ClassCacheUtils.getTableName(tabClass))
                .WHERE(parseWhereParams(wp))
                .toString();
    }

    /**
     * 解析DB列名与类Field对象映射
     *
     * @param clz
     * @return
     */
    private static Map<String, Field> parseColumnFieldMap(Class clz) {
        return ClassCacheUtils.getColumnFieldMap(clz);

    }

    /**
     * 解析SQL where条件查询参数
     *
     * @param o
     * @return
     */
    private static String parseWhereParams(Object o) {
        // where条件不加缓存, 将来可以进行参数化优化
        Field[] declaredFields = o.getClass().getDeclaredFields();

        List<String> whereParams = Lists.newArrayList();

        for (Field declaredField : declaredFields) {
            if (Objects.nonNull(declaredField.getAnnotation(SqlIgnore.class))) {
                continue;
            }
            SqlColumn annotation = declaredField.getAnnotation(SqlColumn.class);
            StringBuilder builder = new StringBuilder();
            try {
                declaredField.setAccessible(true);
                Object value = declaredField.get(o);
                if (Objects.nonNull(value)) {
                    Class type = declaredField.getType();
                    String fieldName = Objects.nonNull(annotation) ? annotation.name() : toUnderscore(declaredField.getName());
                    builder.append(fieldName);
                    if (type == List.class) {
                        if (Objects.equals(annotation.rel(), SqlRelEnum.BETWEEN)) {
                            builder.append(BETWEEN).append(((List) value).get(0)).append(AND).append(((List) value).get(1)).append(SPACE);
                        } else {
                            Class<?> genericType = (Class<?>) ((ParameterizedType) declaredField.getGenericType()).getActualTypeArguments()[0];
                            if (genericType == String.class) {
                                value = ((List) value).stream().map(v -> SINGLE_QUOTAS + v + SINGLE_QUOTAS).collect(Collectors.joining(COMMA));
                            } else if (genericType == Integer.class) {
                                value = ((List) value).stream().map(String::valueOf).collect(Collectors.joining(COMMA));
                            } else {
                                throw new RuntimeException("#parseWhereParams# Unsupported type: " + genericType);
                            }
                            builder.append(IN).append(LEFT_PARENTHESIS).append(value).append(RIGHT_PARENTHESIS);
                        }
                    } else if (type == String.class) {
                        builder.append(EQUAL).append(SINGLE_QUOTAS).append(value).append(SINGLE_QUOTAS);
                    } else {
                        builder.append(EQUAL).append(value);
                    }
                    whereParams.add(builder.toString());
                }
            } catch (IllegalAccessException e) {
                log.error("Failed to get field: {} value from object: {}", declaredField.getName(), o);
                throw new RuntimeException("Failed to get field:" + declaredField.getName() + " value from object: " + o);
            }
        }

        return StringUtils.join(whereParams, AND);
    }

    /**
     * 解析结果对象集合
     *
     * @param clz
     * @param inputStream
     * @param <T>
     * @return
     */
    public static <T> List<T> parseResultList(Class<T> clz, InputStream inputStream) {
        return parseResultList(clz, Lists.newArrayList(Objects.requireNonNull(ClassCacheUtils.getColumnFieldMap(clz)).keySet()), inputStream, null);
    }

    /**
     * 解析结果对象集合
     *
     * @param clz
     * @param columnNames
     * @param inputStream 输入流
     * @param splitterStr
     * @param <T>
     * @return
     */
    public static <T> List<T> parseResultList(Class<T> clz, List<String> columnNames, InputStream inputStream, String splitterStr) {
        List<T> resultList = Lists.newArrayList();

        Map<String, Field> columnFieldMap = parseColumnFieldMap(clz);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while (true) {
                if ((line = reader.readLine()) == null) {
                    break;
                }
                String[] columnsValues = line.split(StringUtils.defaultIfEmpty(splitterStr, COLUMN_SEPARATOR));
                // 校验查询列与输入列相同
                if (Objects.equals(CollectionUtils.size(columnNames), CollectionUtils.size(columnsValues))) {
                    resultList.add(parseResultObject(clz, columnFieldMap, columnNames, Arrays.asList(columnsValues)));
                } else {
                    log.error("#parseResultList# expected columns size not match, cName: {}, cVal: {}", columnNames, columnsValues);
                    return null;
                }
            }
        } catch (IOException e) {
            log.error("#parseResultList# parse exception, class: {}, fields: {}", clz, columnNames, e);
        }
        return resultList;
    }

    /**
     * 解析单值
     *
     * @param clz
     * @param inputStream
     * @param <T>
     * @return
     */
    public static <T> T parseValue(Class<T> clz, InputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = reader.readLine();
            return StringUtils.isNotBlank(line) ? (T) parseValue(clz, line) : null;
        } catch (IOException e) {
            log.error("#parseValue# exception, class: {}", clz, e);
            return null;
        }
    }

    /**
     * 解析单列集合
     *
     * @param clz
     * @param inputStream
     * @param <T>
     * @return
     */
    public static <T> List<T> parseValueList(Class<T> clz, InputStream inputStream) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line = null;
            List<T> resList = Lists.newArrayList();
            while ((line = reader.readLine()) != null) {
                resList.add((T) parseValue(clz, line));
            }
            return resList;
        } catch (IOException e) {
            log.error("#parseValue# exception, class: {}", clz, e);
            return null;
        }
    }

    /**
     * 字符串转换为class类型的对象
     *
     * @param clz
     * @param val
     * @return
     */
    private static Object parseValue(Class clz, Object val) {
        if (Objects.isNull(val)) {
            return null;
        }
        if (val instanceof String) {
            if (String.class.equals(clz)) {
                return val;
            }
            if (StringUtils.isBlank((CharSequence) val)) {
                return null;
            }
            if (Integer.class.equals(clz)) {
                return Integer.parseInt((String) val);
            }
            if (Long.class.equals(clz)) {
                return Long.parseLong((String) val);
            }
            if (Float.class.equals(clz)) {
                return Float.parseFloat((String) val);
            }
            if (Double.class.equals(clz)) {
                return Double.parseDouble((String) val);
            }
            if (Boolean.class.equals(clz)) {
                return Boolean.parseBoolean((String) val);
            }
        }
        throw new RuntimeException("Unsupported type clz: " + clz + " val: " + val);
    }

    /**
     * 解析结果对象集合
     *
     * @param cls
     * @param rows 每行的值集合
     * @param <T>
     * @return
     */
    public static <T> List<T> parseResultList(Class<T> cls, List<List<Object>> rows) {
        Map<String, Field> columnFieldMap = parseColumnFieldMap(cls);
        List<String> columnNames = Lists.newArrayList(Objects.requireNonNull(ClassCacheUtils.getColumnFieldMap(cls)).keySet());
        return rows.stream().map(columnValues -> parseResultObject(cls, columnFieldMap, columnNames, columnValues)).collect(Collectors.toList());
    }

    /**
     * 解析对象
     *
     * @param cls
     * @param columnFieldMap
     * @param columnsNames
     * @param columnValues
     * @param <T>
     * @return
     */
    public static <T> T parseResultObject(Class<T> cls, Map<String, Field> columnFieldMap, List<String> columnsNames, List<Object> columnValues) {
        // 创建对象
        T object = null;
        try {
            object = cls.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            log.error("#parseResultObject# new instance exception, class: {}", cls, e);
            return null;
        }
        for (int i = 0; i < columnsNames.size(); i++) {
            // 忽略空值
            if (Objects.isNull(columnValues.get(i)) || "NULL".equals(((String) columnValues.get(i)).toUpperCase())) {
                continue;
            }
            Field field = columnFieldMap.get(columnsNames.get(i));
            field.setAccessible(true);
            try {
                field.set(object, parseValue(field.getType(), columnValues.get(i)));
            } catch (Exception e) {
                log.error("#parseResultObject# field access exception, field: {}, value: {}", field, columnValues.get(i), e);
                return null;
            }
        }
        return object;
    }

    /**
     * 驼峰转下划线
     *
     * @param camelCase
     * @return
     */
    private static String toUnderscore(String camelCase) {
        return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, camelCase);
    }
}
