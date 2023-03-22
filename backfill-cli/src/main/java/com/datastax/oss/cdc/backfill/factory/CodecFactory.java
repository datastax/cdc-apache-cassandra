/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.oss.cdc.backfill.factory;

import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class loader aware converting codec factory.
 */
@NotThreadSafe
public class CodecFactory {
    /**
     * Works around the dsbulk codec factory limitation that uses the class-loader unaware ServiceLoader.load() API
     * which defaults to Thread.currentThread().getContextClassLoader(). This will lead to class loading issues when
     * ruing as a NAR archive.
     */
    public ConvertingCodecFactory newCodecFactory(ClassLoader classLoader) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            return new ConvertingCodecFactory();
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }
}
