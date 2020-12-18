package com.example.annodemo.service;

import com.example.annodemo.util.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.SQL;
import org.apache.ibatis.session.SqlSession;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;

@Slf4j
@Service
public class SqlBaseService {
    @Resource
    private SqlSessionSupport sqlSessionSupport;

    private SqlSession sqlSession;

    @PostConstruct
    private void init() {
        if (sqlSession == null) {
            sqlSession = sqlSessionSupport.getSqlSession();
        }
    }

    public <T> List<T> queryResultList(Class<T> cls, Object param) {
        return queryResultList(cls, param, null);
    }

    public <T> List<T> queryResultList(Class<T> cls, Object param, List<String> fields) {
        log.info("#queryResultList# param, cls: {}, param: {}", cls, param);
        String sql = SqlUtils.selectSQL(cls, fields, param);

        return queryResultList(sql, cls);
    }

    public <T> List<T> queryResultList(Class<T> resCls, SQL sql) {
        return queryResultList(sql.toString(), resCls);
    }

    public <T> List<T> queryResultList(String sql, Class<T> resCls) {
        log.info("#queryResultList# begin, sql: {}", sql);

        InputStream inputStream = null; //TODO：获取结果

        if (Objects.nonNull(inputStream)) {
            return SqlUtils.parseResultList(resCls, inputStream);
        }
        return null;
    }
}
