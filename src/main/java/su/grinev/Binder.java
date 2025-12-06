package su.grinev;

import annotation.BsonType;
import su.grinev.bson.Document;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Binder {

    private static final Map<Class<?>, Map<String, Field>> fieldCache = new ConcurrentHashMap<>();

    public <T> T bind(Class<T> tClass, Document document) {
        Object rootObject = instantiate(tClass);
        Deque<BinderContext> stack = new LinkedList<>();
        stack.addLast(new BinderContext(rootObject, document.getDocumentMap(), tClass));

        while (!stack.isEmpty()) {
            BinderContext ctx = stack.removeLast();

            if (ctx.o instanceof Map targetMap && ctx.document instanceof Map<?, ?> docMap) {
                targetMap.putAll(docMap);
                continue;
            }

            if (ctx.o instanceof Collection<?> collection && ctx.document instanceof List<?> listData) {
                Type itemType = resolveListItemType(ctx.type);
                for (Object rawItem : listData) {
                    if (isPrimitiveOrWrapperOrString(rawItem.getClass())) {
                        ((Collection<Object>) collection).add(rawItem);
                    } else if (rawItem instanceof Map<?, ?> mapItem) {
                        Class<?> itemClass = resolveClassFromType(itemType);
                        Object itemObj = instantiate(itemClass);
                        ((Collection<Object>) collection).add(itemObj);
                        stack.addLast(new BinderContext(itemObj, mapItem, itemClass));
                    }
                }
                continue;
            }

            Map<String, Field> fieldsMap = collectFields(ctx.o.getClass());
            Map<String, Object> documentMap = (Map<String, Object>) ctx.document;

            for (Map.Entry<String, Object> entry : documentMap.entrySet()) {
                Field field = fieldsMap.get(entry.getKey());
                if (field == null) continue;

                Object value = entry.getValue();

                try {
                    if (isPrimitiveOrWrapperOrString(field.getType())) {
                        field.set(ctx.o, value);
                    } else if (field.getType().isEnum()) {
                        Enum<?> enumValue = Enum.valueOf((Class<Enum>) field.getType(), value.toString());
                        field.set(ctx.o, enumValue);
                    } else if (Collection.class.isAssignableFrom(field.getType())) {
                        Collection<Object> target = instantiateCollection(field.getType());
                        field.set(ctx.o, target);
                        stack.addLast(new BinderContext(target, value, field.getGenericType()));
                    } else if (Map.class.isAssignableFrom(field.getType())) {
                        Map<Object, Object> targetMap = new HashMap<>();
                        field.set(ctx.o, targetMap);
                        stack.addLast(new BinderContext(targetMap, value, field.getGenericType()));
                    } else if (field.isAnnotationPresent(BsonType.class)) {
                        String discriminatorField = field.getAnnotation(BsonType.class).discriminator();
                        String className = (String) documentMap.get(discriminatorField);
                        Class<?> targetCls = Class.forName(className);
                        Object newObject = instantiate(targetCls);
                        field.set(ctx.o, newObject);
                        stack.addLast(new BinderContext(newObject, value, targetCls));
                    } else {
                        Class<?> targetCls = field.getType();
                        Object newObject = instantiate(targetCls);
                        field.set(ctx.o, newObject);
                        stack.addLast(new BinderContext(newObject, value, targetCls));
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to bind field: " + field.getName(), e);
                }
            }
        }

        return (T) rootObject;
    }

    public Document unbind(Object o) {
        Map<String, Object> rootDocument = new HashMap<>();
        Deque<BinderContext> stack = new LinkedList<>();
        stack.addLast(new BinderContext(o, rootDocument, o.getClass()));

        while (!stack.isEmpty()) {
            BinderContext ctx = stack.removeLast();
            Map<String, Object> currentDocument = (Map<String, Object>) ctx.document;
            Map<String, Field> fieldMap = collectFields(ctx.o.getClass());

            try {
                for (Map.Entry<String, Field> fieldEntry : fieldMap.entrySet()) {
                    String name = fieldEntry.getKey();
                    Field field = fieldEntry.getValue();
                    Object fieldValue = field.get(ctx.o);
                    if (fieldValue == null) continue;

                    if (isPrimitiveOrWrapperOrString(field.getType())) {
                        currentDocument.put(name, fieldValue);
                    } else if (field.getType().isEnum()) {
                        currentDocument.put(name, fieldValue.toString());
                    } else if (field.isAnnotationPresent(BsonType.class)) {
                        Map<String, Object> nested = new LinkedHashMap<>();
                        String discriminator = field.getAnnotation(BsonType.class).discriminator();
                        currentDocument.put(discriminator, fieldValue.getClass().getName());
                        currentDocument.put(name, nested);
                        stack.addLast(new BinderContext(fieldValue, nested, fieldValue.getClass()));
                    } else if (Collection.class.isAssignableFrom(field.getType())) {
                        List<Object> serialized = new ArrayList<>();
                        currentDocument.put(name, serialized);
                        for (Object item : (Collection<?>) fieldValue) {
                            if (isPrimitiveOrWrapperOrString(item.getClass()) || item.getClass().isEnum()) {
                                serialized.add(item.toString());
                            } else {
                                Map<String, Object> nested = new LinkedHashMap<>();
                                serialized.add(nested);
                                stack.addLast(new BinderContext(item, nested, item.getClass()));
                            }
                        }
                    } else if (Map.class.isAssignableFrom(field.getType())) {
                        Map<String, Object> nestedMap = new LinkedHashMap<>();
                        currentDocument.put(name, nestedMap);
                        Map<?, ?> sourceMap = (Map<?, ?>) fieldValue;
                        sourceMap.forEach((k, v) -> nestedMap.put(k.toString(), v));
                    } else {
                        Map<String, Object> nested = new LinkedHashMap<>();
                        currentDocument.put(name, nested);
                        stack.addLast(new BinderContext(fieldValue, nested, fieldValue.getClass()));
                    }
                }
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return new Document(rootDocument, 0);
    }

    private Object instantiate(Class<?> clazz) {
        try {
            Constructor<?> ctor = clazz.getDeclaredConstructor();
            ctor.setAccessible(true);
            return ctor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Collection<Object> instantiateCollection(Class<?> type) {
        if (type.isAssignableFrom(List.class) || type.isAssignableFrom(ArrayList.class)) {
            return new ArrayList<>();
        }
        if (type.isAssignableFrom(Set.class) || type.isAssignableFrom(HashSet.class)) {
            return new HashSet<>();
        }
        if (type.isAssignableFrom(Queue.class) || type.isAssignableFrom(LinkedList.class)) {
            return new LinkedList<>();
        }
        throw new UnsupportedOperationException("Unsupported collection type: " + type);
    }

    private static Map<String, Field> collectFields(Class<?> clazz) {
        return fieldCache.computeIfAbsent(clazz, c ->
                Arrays.stream(c.getDeclaredFields())
                        .peek(f -> f.setAccessible(true))
                        .collect(Collectors.toMap(Field::getName, f -> f))
        );
    }

    public static boolean isPrimitiveOrWrapperOrString(Class<?> type) {
        return type.isPrimitive()
                || type == Instant.class
                || type == LocalDateTime.class
                || type == BigDecimal.class
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
                || type == Enum.class
                || type == ByteBuffer.class;
    }

    private Type resolveListItemType(Type listType) {
        if (listType instanceof ParameterizedType pt) {
            return pt.getActualTypeArguments()[0];
        }
        return Object.class;
    }

    private Class<?> resolveClassFromType(Type type) {
        if (type instanceof Class<?> c) return c;
        if (type instanceof ParameterizedType pt) return (Class<?>) pt.getRawType();
        throw new IllegalArgumentException("Unknown type: " + type);
    }

    private record BinderContext(Object o, Object document, Type type) {}
}
