package com.example.annodemo.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.example.annodemo.common.PropertyFactory;
import com.example.annodemo.constant.Constant;
import com.example.annodemo.mapper.IUserMapper;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Properties;

@Component
public class SqlSessionSupport {

    public SqlSession getSqlSession() {
        return getSqlSessionFactory().openSession();
    }

    private SqlSessionFactory getSqlSessionFactory() {
        TransactionFactory transactionFactory = new JdbcTransactionFactory();
        Environment environment = new Environment("development", transactionFactory, getDataSource());
        Configuration configuration = new Configuration(environment);
        configuration.addMapper(IUserMapper.class);
        SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(configuration);
        return sqlSessionFactory;
    }

    private DruidDataSource getDataSource() {
        Properties properties = PropertyFactory.getProperties();
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl(properties.getProperty(Constant.JDBC_URL));
        dataSource.setDriverClassName(properties.getProperty(Constant.DRIVER_CLASS));
        dataSource.setUsername(properties.getProperty(Constant.USER));
        dataSource.setPassword(properties.getProperty(Constant.PASSWORD));
        try {
            dataSource.init();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return dataSource;
    }
}
