package su.grinev;

import annotation.BsonType;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class Binder {

    public <T> T bind(Class<T> tClass, Map<String, Object> document) {
        Object rootObject = instantiate(tClass);
        Deque<BinderContext> stack = new LinkedList<>();
        stack.addLast(new BinderContext(rootObject, document));

        while (!stack.isEmpty()) {
            BinderContext ctx = stack.removeLast();

            if (ctx.o instanceof Map m) {
                m.forEach((key, value) -> {
                    if (isPrimitiveOrWrapperOrString(value.getClass())) {
                        m.put(ctx.o, value);
                    } else if (value instanceof List) {
                        List list = new ArrayList();
                        m.put(key, list);
                        stack.addLast(new BinderContext(list, value));
                    } else if (value instanceof Map) {
                        Map map = new HashMap();
                        m.put(ctx.o, map);
                        stack.addLast(new BinderContext(map, value));
                    }
                });
            } else {
                Map<String, Field> fieldsMap = collectFields(ctx.o.getClass());
                Map<String, Object> documentMap = ((Map<String, Object>) ctx.document);

                for (Map.Entry<String, Object> entry : documentMap.entrySet()) {
                    if (fieldsMap.get(entry.getKey()) == null) {
                        continue;
                    }
                    String key = entry.getKey();
                    Object value = entry.getValue();

                    try {
                        if (isPrimitiveOrWrapperOrString(fieldsMap.get(key).getType())) {
                            fieldsMap.get(key).set(ctx.o, value);
                        } else if (fieldsMap.get(key).getType().equals(List.class)) {
                            List list = new ArrayList();
                            fieldsMap.get(key).set(ctx.o, list);
                            stack.addLast(new BinderContext(list, value));
                        } else if (fieldsMap.get(key).getType().equals(Map.class)) {
                            Map map = new HashMap();
                            fieldsMap.get(key).set(ctx.o, map);
                            stack.addLast(new BinderContext(map, value));
                        } else if (fieldsMap.get(key).isAnnotationPresent(BsonType.class)) {
                            String discriminatorField = fieldsMap.get(key).getAnnotation(BsonType.class).discriminator();
                            String className = (String) documentMap.get(discriminatorField);
                            Class<?> targetCls = Class.forName(className);
                            Object newObject = instantiate(targetCls);
                            fieldsMap.get(key).set(ctx.o, newObject);
                            stack.addLast(new BinderContext(newObject, value));
                        }
                    } catch (IllegalAccessException | ClassNotFoundException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
        return (T) rootObject;
    }

    public Map<String, Object> unbind(Object o) {
        Map<String, Object> rootDocument = new HashMap<>();
        Deque<BinderContext> stack = new LinkedList<>();
        stack.addLast(new BinderContext(o, rootDocument));

        while (!stack.isEmpty()) {
            BinderContext ctx = stack.removeLast();
            Map<String, Object> currentDocument = (Map<String, Object>) ctx.document;
            Map<String, Field> fieldMap = collectFields(ctx.o.getClass());

            try {
                for (Map.Entry<String, Field> field : fieldMap.entrySet()) {
                    if (currentDocument.get(field.getKey()) != null) {
                        continue;
                    }

                    if (isPrimitiveOrWrapperOrString(field.getValue().getType())) {
                        currentDocument.put(field.getKey(), field.getValue().get(ctx.o));
                    } else if (field.getValue().getType() == Map.class) {
                        Map<String, Object> nestedDocument = new LinkedHashMap<>();
                        currentDocument.put(field.getKey(), nestedDocument);
                        stack.addLast(new BinderContext(field.getValue().get(ctx.o), nestedDocument));
                    } else if (field.getValue().isAnnotationPresent(BsonType.class)) {
                        Map<String, Object> nestedDocument = new LinkedHashMap<>();
                        String discriminator = field.getValue().getAnnotation(BsonType.class).discriminator();
                        currentDocument.put(discriminator, field.getValue().get(ctx.o).getClass().getName());
                        currentDocument.put(field.getKey(), nestedDocument);
                        stack.addLast(new BinderContext(field.getValue().get(ctx.o), nestedDocument));
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return rootDocument;
    }

    private Object instantiate(Class<?> clazz) {
        try {
            Constructor<?> ctor = clazz.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, Field> collectFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .peek(f -> f.setAccessible(true))
                .collect(Collectors.toMap(
                        Field::getName,
                        f -> f
                ));
    }

    public static boolean isPrimitiveOrWrapperOrString(Class<?> type) {
        return type.isPrimitive()
                || type == Boolean.class
                || type == Byte.class
                || type == Short.class
                || type == Integer.class
                || type == Long.class
                || type == Float.class
                || type == Double.class
                || type == Character.class
                || type == String.class
                || type == byte[].class
                || type == short[].class
                || type == int[].class
                || type == long[].class
                || type == float[].class
                || type == double[].class
                || type == boolean[].class
                || type == char[].class
                || type == Boolean[].class
                || type == Byte[].class
                || type == Short[].class
                || type == Integer[].class
                || type == Long[].class
                || type == Float[].class
                || type == Double[].class
                || type == Character[].class
                || type == String[].class
                || List.class.isAssignableFrom(type)
                || Map.class.isAssignableFrom(type);
    }

    record BinderContext(Object o, Object document) {}
}
