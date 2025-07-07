package su.grinev;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;

public class PojoBinderImpl implements Binder {
    private final Object newObject;
    private Object currentObject;
    private Map<String, Field> fieldsMap;
    private LinkedList<BinderContext> parents = new LinkedList<>();

    public PojoBinderImpl(Class<?> clazz) {
        newObject = instantiate(clazz);
        currentObject = newObject;
        fieldsMap = collectFields(clazz);
    }

    public void enter(String key) {
        if ("".equalsIgnoreCase(key) || key == null) {
            return;
        }
        Object nested;
        parents.addLast(new BinderContext(currentObject, fieldsMap));
        try {
            nested = fieldsMap.get(key).get(currentObject);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        if (nested == null) {
            if (fieldsMap.get(key).getType().equals(Map.class)) {
                nested = new HashMap();
                try {
                    fieldsMap.get(key).set(currentObject, nested);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else if (fieldsMap.get(key).getType().equals(List.class)) {
                nested = new ArrayList();
                try {
                    fieldsMap.get(key).set(currentObject, nested);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (fieldsMap.get(key).getType().equals(Object.class)) {
                    try {
                        String className = (String) fieldsMap.get("_class").get(currentObject);

                        nested = instantiate(Class.forName(className));
                    } catch (IllegalAccessException | ClassNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    nested = instantiate(fieldsMap.get(key).getType());
                    try {
                        fieldsMap.get(key).set(currentObject, nested);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
                fieldsMap = collectFields(nested.getClass());
            }
        }
        currentObject = nested;
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

    public void leave() {
        currentObject = parents.getLast().o;
        fieldsMap = parents.getLast().fieldsMap;
        parents.removeLast();
    }

    @Override
    public void bind(KeyTypeValue keyTypeValue) {
        if (keyTypeValue.key().startsWith("_")) {
            return;
        }
        try {
             if (currentObject instanceof Map m) {
                 m.put(keyTypeValue.key(), keyTypeValue.value());
             } else if (currentObject instanceof List l) {
                 l.add(keyTypeValue.value());
             } else {
                 if (keyTypeValue.type().equals("base")) {
                     fieldsMap.get(keyTypeValue.key()).set(currentObject, keyTypeValue.value());
                 } else if (keyTypeValue.type().equals("object")) {
                     if (fieldsMap.get(keyTypeValue.key()).getType().equals(Map.class)) {
                         fieldsMap.get(keyTypeValue.key()).set(currentObject, new HashMap<>());
                     } else {
                         String className = (String) fieldsMap.get("_class").get(currentObject);
                         Object value = instantiate(Class.forName(className));
                         fieldsMap.get(keyTypeValue.key()).set(currentObject, value);
                     }
                 } else {
                     throw new IllegalArgumentException("Unknown type %s".formatted(keyTypeValue.type()));
                 }
             }
        } catch (IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T getObject() {
        return (T) newObject;
    }

    private static Map<String, Field> collectFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .peek(f -> f.setAccessible(true))
                .collect(Collectors.toMap(
                        Field::getName,
                        f -> f
                ));
    }

    record BinderContext(Object o, Map<String, Field> fieldsMap) {}
}
