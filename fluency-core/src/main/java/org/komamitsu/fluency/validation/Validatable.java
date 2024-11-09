/*
 * Copyright 2019 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.komamitsu.fluency.validation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import org.komamitsu.fluency.validation.annotation.DecimalMax;
import org.komamitsu.fluency.validation.annotation.DecimalMin;
import org.komamitsu.fluency.validation.annotation.Max;
import org.komamitsu.fluency.validation.annotation.Min;

public interface Validatable {
  class ValidationList {
    private static class Validation {
      Class<? extends Annotation> annotationClass;
      BiFunction<Annotation, Number, Boolean> isValid;
      String messageTemplate;

      Validation(
          Class<? extends Annotation> annotationClass,
          BiFunction<Annotation, Number, Boolean> isValid,
          String messageTemplate) {
        this.annotationClass = annotationClass;
        this.isValid = isValid;
        this.messageTemplate = messageTemplate;
      }
    }

    private static final Validation VALIDATION_MAX =
        new Validation(
            Max.class,
            (annotation, actual) -> {
              Max maxAnnotation = (Max) annotation;
              if (maxAnnotation.inclusive()) {
                return maxAnnotation.value() >= actual.longValue();
              } else {
                return maxAnnotation.value() > actual.longValue();
              }
            },
            "This field (%s) is more than (%s)");

    private static final Validation VALIDATION_MIN =
        new Validation(
            Min.class,
            (annotation, actual) -> {
              Min minAnnotation = (Min) annotation;
              if (minAnnotation.inclusive()) {
                return minAnnotation.value() <= actual.longValue();
              } else {
                return minAnnotation.value() < actual.longValue();
              }
            },
            "This field (%s) is less than (%s)");

    private static final Validation VALIDATION_DECIMAL_MAX =
        new Validation(
            DecimalMax.class,
            (annotation, actual) -> {
              DecimalMax maxAnnotation = (DecimalMax) annotation;
              BigDecimal limitValue = new BigDecimal(maxAnnotation.value());
              BigDecimal actualValue = new BigDecimal(actual.toString());
              if (maxAnnotation.inclusive()) {
                return limitValue.compareTo(actualValue) >= 0;
              } else {
                return limitValue.compareTo(actualValue) > 0;
              }
            },
            "This field (%s) is more than (%s)");

    private static final Validation VALIDATION_DECIMAL_MIN =
        new Validation(
            DecimalMin.class,
            (annotation, actual) -> {
              DecimalMin maxAnnotation = (DecimalMin) annotation;
              BigDecimal limitValue = new BigDecimal(maxAnnotation.value());
              BigDecimal actualValue = new BigDecimal(actual.toString());
              if (maxAnnotation.inclusive()) {
                return limitValue.compareTo(actualValue) <= 0;
              } else {
                return limitValue.compareTo(actualValue) < 0;
              }
            },
            "This field (%s) is less than (%s)");

    private static final List<Validation> VALIDATIONS =
        Arrays.asList(
            VALIDATION_MAX, VALIDATION_MIN, VALIDATION_DECIMAL_MAX, VALIDATION_DECIMAL_MIN);
  }

  default void validate() {
    Class<? extends Object> klass = getClass();
    while (klass != Object.class) {
      for (Field field : klass.getDeclaredFields()) {
        for (ValidationList.Validation validation : ValidationList.VALIDATIONS) {
          Class<? extends Annotation> annotationClass = validation.annotationClass;
          if (field.isAnnotationPresent(annotationClass)) {
            Annotation annotation = field.getAnnotation(annotationClass);
            Object value;
            try {
              field.setAccessible(true);
              value = field.get(this);
            } catch (IllegalAccessException e) {
              throw new RuntimeException(
                  String.format("Failed to get a value from field (%s)", field), e);
            }

            if (value == null) {
              break;
            }

            if (!(value instanceof Number)) {
              throw new IllegalArgumentException(
                  String.format(
                      "This field has (%s), but actual field is (%s)",
                      annotation, value.getClass()));
            }

            if (!validation.isValid.apply(annotation, (Number) value)) {
              throw new IllegalArgumentException(
                  String.format(validation.messageTemplate, field, value));
            }
          }
        }
      }
      klass = klass.getSuperclass();
    }
  }
}
