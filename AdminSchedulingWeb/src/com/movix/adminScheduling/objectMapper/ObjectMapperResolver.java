package com.movix.adminScheduling.objectMapper;

import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.ANY;
import static org.codehaus.jackson.annotate.JsonAutoDetect.Visibility.NONE;
import static org.codehaus.jackson.annotate.JsonTypeInfo.As.PROPERTY;
import static org.codehaus.jackson.map.ObjectMapper.DefaultTyping.NON_FINAL;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.introspect.VisibilityChecker;


/**
 * @author Angel Nunez
 */
@Provider
public class ObjectMapperResolver implements ContextResolver<ObjectMapper> {

    private ObjectMapper objectMapper;

    public ObjectMapperResolver() {
        objectMapper = new ObjectMapper();
        objectMapper.setVisibilityChecker(new VisibilityChecker.Std(NONE, NONE, NONE, NONE, ANY));
        objectMapper.enableDefaultTyping(NON_FINAL, PROPERTY);
    }

    @Override
    public ObjectMapper getContext(Class<?> aClass) {
        return objectMapper;
    }
}