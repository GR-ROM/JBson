package su.grinev;

import annotation.BsonType;
import annotation.Tag;
import annotation.Transient;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Binder {

    enum FieldKind { PRIMITIVE, ENUM, COLLECTION, MAP, BSON_TYPE, NESTED }

    static final class FieldBinding {
        final int tag;
        final VarHandle handle;
        final FieldKind kind;
        final Class<?> fieldType;
        final Type genericType;
        final int bsonDiscriminator; // -1 if not BSON_TYPE

        FieldBinding(int tag, VarHandle handle, FieldKind kind, Class<?> fieldType, Type genericType, int bsonDiscriminator) {
            this.tag = tag;
            this.handle = handle;
            this.kind = kind;
            this.fieldType = fieldType;
            this.genericType = genericType;
            this.bsonDiscriminator = bsonDiscriminator;
        }
    }

    static final class ClassSchema {
        final FieldBinding[] bindings;   // for iteration in unbind()
        final FieldBinding[] tagLookup;  // tag-indexed array for O(1) lookup in bind()

        ClassSchema(FieldBinding[] bindings, FieldBinding[] tagLookup) {
            this.bindings = bindings;
            this.tagLookup = tagLookup;
        }
    }

    private static final Map<Class<?>, ClassSchema> schemaCache = new ConcurrentHashMap<>();
    private static final Map<Class<?>, MethodHandle> ctorCache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public <T> T bind(Class<T> tClass, BinaryDocument document) {
        Object rootObject = instantiate(tClass);
        ArrayDeque<BinderContext> stack = new ArrayDeque<>();
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

            ClassSchema schema = getSchema(ctx.o.getClass());
            Map<Integer, Object> documentMap = (Map<Integer, Object>) ctx.document;
            FieldBinding[] tagLookup = schema.tagLookup;

            for (Map.Entry<Integer, Object> entry : documentMap.entrySet()) {
                int key = entry.getKey();
                if (key < 0 || key >= tagLookup.length) continue;
                FieldBinding binding = tagLookup[key];
                if (binding == null) continue;

                Object value = entry.getValue();

                try {
                    switch (binding.kind) {
                        case PRIMITIVE -> binding.handle.set(ctx.o, coerceNumeric(binding.fieldType, value));
                        case ENUM -> {
                            Enum<?> enumValue = Enum.valueOf((Class<Enum>) binding.fieldType, value.toString());
                            binding.handle.set(ctx.o, enumValue);
                        }
                        case COLLECTION -> {
                            Collection<Object> target = instantiateCollection(binding.fieldType);
                            binding.handle.set(ctx.o, target);
                            stack.addLast(new BinderContext(target, value, binding.genericType));
                        }
                        case MAP -> {
                            Map<Object, Object> targetMap = new HashMap<>();
                            binding.handle.set(ctx.o, targetMap);
                            stack.addLast(new BinderContext(targetMap, value, binding.genericType));
                        }
                        case BSON_TYPE -> {
                            String className = (String) documentMap.get(binding.bsonDiscriminator);
                            Class<?> targetCls = Class.forName(className);
                            Object newObject = instantiate(targetCls);
                            binding.handle.set(ctx.o, newObject);
                            stack.addLast(new BinderContext(newObject, value, targetCls));
                        }
                        case NESTED -> {
                            Class<?> targetCls = binding.fieldType;
                            Object newObject = instantiate(targetCls);
                            binding.handle.set(ctx.o, newObject);
                            stack.addLast(new BinderContext(newObject, value, targetCls));
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Failed to bind tag: " + key, e);
                }
            }
        }

        return (T) rootObject;
    }

    public BinaryDocument unbind(Object o) {
        Map<Integer, Object> rootDocument = new HashMap<>();
        ArrayDeque<BinderContext> stack = new ArrayDeque<>();
        stack.addLast(new BinderContext(o, rootDocument, o.getClass()));

        while (!stack.isEmpty()) {
            BinderContext ctx = stack.removeLast();
            Map<Integer, Object> currentDocument = (Map<Integer, Object>) ctx.document;
            ClassSchema schema = getSchema(ctx.o.getClass());

            try {
                for (FieldBinding binding : schema.bindings) {
                    Object fieldValue = binding.handle.get(ctx.o);
                    if (fieldValue == null) continue;

                    int tag = binding.tag;
                    switch (binding.kind) {
                        case PRIMITIVE -> currentDocument.put(tag, fieldValue);
                        case ENUM -> currentDocument.put(tag, fieldValue.toString());
                        case BSON_TYPE -> {
                            Map<Integer, Object> nested = new LinkedHashMap<>();
                            currentDocument.put(binding.bsonDiscriminator, fieldValue.getClass().getName());
                            currentDocument.put(tag, nested);
                            stack.addLast(new BinderContext(fieldValue, nested, fieldValue.getClass()));
                        }
                        case COLLECTION -> {
                            List<Object> serialized = new ArrayList<>();
                            currentDocument.put(tag, serialized);
                            for (Object item : (Collection<?>) fieldValue) {
                                if (isPrimitiveOrWrapperOrString(item.getClass()) || item.getClass().isEnum()) {
                                    serialized.add(item.toString());
                                } else {
                                    Map<Integer, Object> nested = new LinkedHashMap<>();
                                    serialized.add(nested);
                                    stack.addLast(new BinderContext(item, nested, item.getClass()));
                                }
                            }
                        }
                        case MAP -> {
                            Map<Integer, Object> nestedMap = new LinkedHashMap<>();
                            currentDocument.put(tag, nestedMap);
                            Map<?, ?> sourceMap = (Map<?, ?>) fieldValue;
                            sourceMap.forEach((k, v) -> nestedMap.put(((Number) k).intValue(), v));
                        }
                        case NESTED -> {
                            Map<Integer, Object> nested = new LinkedHashMap<>();
                            currentDocument.put(tag, nested);
                            stack.addLast(new BinderContext(fieldValue, nested, fieldValue.getClass()));
                        }
                    }
                }
            } catch (RuntimeException e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        return new BinaryDocument(rootDocument, 0);
    }

    private static Object instantiate(Class<?> clazz) {
        try {
            MethodHandle ctor = ctorCache.computeIfAbsent(clazz, c -> {
                try {
                    MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(c, MethodHandles.lookup());
                    return lookup.findConstructor(c, MethodType.methodType(void.class));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            return ctor.invoke();
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable e) {
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

    private static ClassSchema getSchema(Class<?> clazz) {
        return schemaCache.computeIfAbsent(clazz, Binder::buildSchema);
    }

    private static ClassSchema buildSchema(Class<?> clazz) {
        MethodHandles.Lookup lookup;
        try {
            lookup = MethodHandles.privateLookupIn(clazz, MethodHandles.lookup());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        List<FieldBinding> bindingList = new ArrayList<>();
        int maxTag = -1;

        for (Field field : clazz.getDeclaredFields()) {
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }
            Tag tag = field.getAnnotation(Tag.class);
            if (tag == null) {
                throw new IllegalArgumentException(
                        "Field '" + field.getName() + "' in class '" + clazz.getName()
                                + "' must be annotated with @Tag or @Transient");
            }
            if (tag.value() < 0) {
                throw new IllegalArgumentException(
                        "Tag value for field '" + field.getName() + "' in class '" + clazz.getName()
                                + "' must be non-negative, got " + tag.value());
            }

            VarHandle handle;
            try {
                handle = lookup.findVarHandle(clazz, field.getName(), field.getType());
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            FieldKind kind;
            int bsonDiscriminator = -1;
            Class<?> fieldType = field.getType();

            if (isPrimitiveOrWrapperOrString(fieldType)) {
                kind = FieldKind.PRIMITIVE;
            } else if (fieldType.isEnum()) {
                kind = FieldKind.ENUM;
            } else if (Collection.class.isAssignableFrom(fieldType)) {
                kind = FieldKind.COLLECTION;
            } else if (Map.class.isAssignableFrom(fieldType)) {
                kind = FieldKind.MAP;
            } else if (field.isAnnotationPresent(BsonType.class)) {
                kind = FieldKind.BSON_TYPE;
                bsonDiscriminator = field.getAnnotation(BsonType.class).discriminator();
            } else {
                kind = FieldKind.NESTED;
            }

            FieldBinding binding = new FieldBinding(tag.value(), handle, kind, fieldType, field.getGenericType(), bsonDiscriminator);
            bindingList.add(binding);

            if (tag.value() > maxTag) {
                maxTag = tag.value();
            }
        }

        // Check for duplicate tags
        FieldBinding[] tagLookup = new FieldBinding[maxTag + 1];
        for (FieldBinding binding : bindingList) {
            if (tagLookup[binding.tag] != null) {
                throw new IllegalArgumentException(
                        "Duplicate tag value " + binding.tag + " in class '" + clazz.getName() + "'");
            }
            tagLookup[binding.tag] = binding;
        }

        return new ClassSchema(bindingList.toArray(new FieldBinding[0]), tagLookup);
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

    private static Object coerceNumeric(Class<?> targetType, Object value) {
        if (value instanceof Number num) {
            if (targetType == Long.class || targetType == long.class) return num.longValue();
            if (targetType == Integer.class || targetType == int.class) return num.intValue();
            if (targetType == Double.class || targetType == double.class) return num.doubleValue();
            if (targetType == Float.class || targetType == float.class) return num.floatValue();
            if (targetType == Short.class || targetType == short.class) return num.shortValue();
            if (targetType == Byte.class || targetType == byte.class) return num.byteValue();
        }
        return value;
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
