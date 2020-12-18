package com.example.annodemo.annotation;

import com.example.annodemo.enums.SqlRelEnum;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface SqlColumn {
    String name() default "";

    SqlRelEnum rel() default SqlRelEnum.EQ;
}
