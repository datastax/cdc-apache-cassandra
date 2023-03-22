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
