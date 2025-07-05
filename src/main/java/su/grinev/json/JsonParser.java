package su.grinev.json;

import su.grinev.json.token.NumberToken;
import su.grinev.json.token.StringToken;
import su.grinev.json.token.Token;

import java.util.*;

import static su.grinev.json.ParserState.*;
import static su.grinev.json.token.TokenType.*;

public class JsonParser {

    public Map<String, Object> parseObject(List<Token> tokenList) {
        int pos;
        Map<String, Object> rootObject = new HashMap<>();
        LinkedList<ParserContext> stack = new LinkedList<>();
        stack.add(new ParserContext(0, rootObject));

        while (!stack.isEmpty()) {
            ParserContext context = stack.removeLast();
            pos = context.pos + 1;
            if (context.value instanceof Map object) {
                doParseObject(tokenList, object, pos, stack);
            } else if (context.value instanceof List object) {
                doParseArray(tokenList, object, pos, stack);
            }
        }

        return rootObject;
    }

    public List<Object> parseArray(List<Token> tokenList) {
        int pos;
        List<Object> rootArray = new ArrayList<>();
        LinkedList<ParserContext> stack = new LinkedList<>();
        stack.add(new ParserContext(0, rootArray));

        while (!stack.isEmpty()) {
            ParserContext context = stack.removeLast();

            pos = context.pos + 1;
            if (context.value instanceof Map object) {
                 doParseObject(tokenList, object, pos, stack);
            } else if (context.value instanceof List object) {
                doParseArray(tokenList, object, pos, stack);
            }
        }
        return rootArray;
    }

    private static void doParseObject(List<Token> tokenList, Map<String, Object> object, int pos, List<ParserContext> stack) {
        ParserState state = EXPECT_KEY;
        String key = null;

        while (pos < tokenList.size()) {
            Token token = tokenList.get(pos++);

            if (token.getType() == STRING && state == EXPECT_KEY) {
                key = ((StringToken) token).getString();
                state = EXPECT_COLON;
                continue;
            }

            if (token.getType() == COLON && state == EXPECT_COLON) {
                state = EXPECT_VALUE;
                continue;
            }

            if (state == EXPECT_VALUE) {
                Object value = getValue(token, pos, stack);
                object.put(key, value);
                key = null;
                state = EXPECT_COMMA_OR_CURLY_CLOSE;
                continue;
            }

            if (state == EXPECT_COMMA_OR_CURLY_CLOSE) {
                if (token.getType() == COMMA) {
                    state = EXPECT_KEY;
                    continue;
                }

                if (token.getType() == CURLY_CLOSE) {
                    break;
                }
            }
        }
    }

    private static void doParseArray(List<Token> tokenList, List<Object> object, int pos, List<ParserContext> stack) {
        ParserState state = EXPECT_VALUE;

        while (pos < tokenList.size()) {
            Token token = tokenList.get(pos++);

            if (state == EXPECT_VALUE) {
                Object value = getValue(token, pos, stack);
                object.add(value);
                state = EXPECT_COMMA_OR_CURLY_CLOSE;
                continue;
            }
            if (state == EXPECT_COMMA_OR_CURLY_CLOSE) {
                if (COMMA.equals(token.getType())) {
                    state = EXPECT_VALUE;
                    continue;
                }
                if (SQUARE_CLOSE.equals(token.getType())) {
                    break;
                }
            }
        }
    }

    private static Object getValue(Token token, int pos, List<ParserContext> stack) {
        return switch (token.getType()) {
            case STRING -> ((StringToken) token).getString();
            case NUMBER -> ((NumberToken) token).getNumber();
            case TRUE -> Boolean.TRUE;
            case FALSE -> Boolean.FALSE;
            case NULL -> null;
            case CURLY_OPEN -> {
                Map<String, Object> nestedObject = new HashMap<>();
                stack.add(new ParserContext(pos - 1, nestedObject));
                yield nestedObject;
            }
            case SQUARE_OPEN -> {
                List<Object> nestedArray = new ArrayList<>();
                stack.add(new ParserContext(pos - 1, nestedArray));
                yield nestedArray;
            }
            default -> throw new IllegalArgumentException("Unknown token type");
        };
    }

    private record ParserContext(int pos, Object value) { }
}