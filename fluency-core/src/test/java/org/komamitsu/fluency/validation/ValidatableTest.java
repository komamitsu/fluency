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

import org.junit.jupiter.api.Test;
import org.komamitsu.fluency.validation.annotation.Max;
import org.komamitsu.fluency.validation.annotation.Min;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ValidatableTest
{
    private static class MaxTest
        implements Validatable
    {
        @Max(42)
        private final int i;

        @Max(Integer.MAX_VALUE)
        private final Long l;

        @Max(value = 42, inclusive = false)
        private final int exclusive;

        public MaxTest(int i, Long l, int exclusive)
        {
            this.i = i;
            this.l = l;
            this.exclusive = exclusive;
        }
    }

    private static class MinTest
        implements Validatable
    {
        @Min(42)
        private final int i;

        @Min(Integer.MIN_VALUE)
        private final Long l;

        @Min(value = 42, inclusive = false)
        private final int exclusive;

        public MinTest(int i, Long l, int exclusive)
        {
            this.i = i;
            this.l = l;
            this.exclusive = exclusive;
        }
    }

    @Test
    void validateMax()
    {
        new MaxTest(42, (long) Integer.MAX_VALUE, 41).validate();

        assertThrows(IllegalArgumentException.class,
                () -> new MaxTest(43, (long) Integer.MAX_VALUE, 41).validate());

        assertThrows(IllegalArgumentException.class,
                () -> new MaxTest(42, (long) Integer.MAX_VALUE + 1, 41).validate());

        assertThrows(IllegalArgumentException.class,
                () -> new MaxTest(42, (long) Integer.MAX_VALUE, 42).validate());

        new MaxTest(42, null, 41).validate();
    }

    @Test
    void validateMin()
    {
        new MinTest(42, (long) Integer.MIN_VALUE, 43).validate();

        assertThrows(IllegalArgumentException.class,
                () -> new MinTest(41, (long) Integer.MIN_VALUE, 43).validate());

        assertThrows(IllegalArgumentException.class,
                () -> new MinTest(42, (long) Integer.MIN_VALUE - 1, 43).validate());

        assertThrows(IllegalArgumentException.class,
                () -> new MinTest(42, (long) Integer.MIN_VALUE, 42).validate());

        new MinTest(42, null, 43).validate();
    }
}