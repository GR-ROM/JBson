package su.grinev.json;

import su.grinev.json.token.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static su.grinev.json.token.TokenType.*;

public class Tokenizer {
    private static final char[] TRUE = "true".toCharArray();
    private static final char[] FALSE = "false".toCharArray();
    private static final char[] NULL = "null".toCharArray();
    private final Buffer buffer;
    private final StringParser stringParser;

    public Tokenizer(byte[] jsonString) {
        this.buffer = new Buffer(ByteBuffer.wrap(jsonString));
        this.stringParser = new StringParser(buffer);
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        while (buffer.hasNext()) {
            skipWhitespace();
            if (!buffer.hasNext()) break;
            char c = buffer.peek();

            Token token = switch (c) {
                case '{' -> { buffer.next(); yield new Token(CURLY_OPEN); }
                case '}' -> { buffer.next(); yield new Token(CURLY_CLOSE); }
                case '[' -> { buffer.next(); yield new Token(SQUARE_OPEN); }
                case ']' -> { buffer.next(); yield new Token(SQUARE_CLOSE); }
                case ':' -> { buffer.next(); yield new Token(COLON); }
                case ',' -> { buffer.next(); yield new Token(COMMA); }
                case 't' -> parseLiteral(TRUE, TokenType.TRUE);
                case 'f' -> parseLiteral(FALSE, TokenType.FALSE);
                case 'n' -> parseLiteral(NULL, TokenType.NULL);
                case '"' -> stringParser.parseString();
                default -> {
                    if ((c == '-') || (c >= '0' && c <= '9')) {
                        yield parseNumber();
                    }
                    throw new IllegalArgumentException("Unexpected character at pos: %s character: '%s'".formatted(buffer.getPos(), c));
                }
            };

            tokens.add(token);
        }
        return tokens;
    }

    private void skipWhitespace() {
        while (buffer.hasNext() && Character.isWhitespace(buffer.peek())) {
            buffer.next();
        }
    }

    private Token parseLiteral(char[] expected, TokenType type) {
        for (char ec : expected) {
            if (!buffer.hasNext() || buffer.next() != ec) {
                throw new IllegalArgumentException("Invalid literal at pos: " + buffer.getPos());
            }
        }
        return new Token(type);
    }

    private NumberToken parseNumber() {
        StringBuilder sb = new StringBuilder();
        if (buffer.peek() == '-') sb.append(buffer.next());

        sb.append(parseIntPart());

        if (buffer.hasNext() && buffer.peek() == '.') {
            sb.append(buffer.next());
            sb.append(parseFractionPart());
        }

        if (buffer.hasNext() && (buffer.peek() == 'e' || buffer.peek() == 'E')) {
            sb.append(buffer.next());
            sb.append(parseExponentPart());
        }

        return new NumberToken(Float.parseFloat(sb.toString()));
    }

    private String parseIntPart() {
        StringBuilder sb = new StringBuilder();
        if (buffer.peek() == '0') {
            sb.append(buffer.next());
        } else {
            while (buffer.hasNext() && Character.isDigit(buffer.peek())) {
                sb.append(buffer.next());
            }
        }
        return sb.toString();
    }

    private String parseFractionPart() {
        StringBuilder sb = new StringBuilder();
        while (buffer.hasNext() && Character.isDigit(buffer.peek())) {
            sb.append(buffer.next());
        }

        if (sb.isEmpty()) {
            throw new IllegalArgumentException("Invalid fraction at pos: " + buffer.getPos());
        }
        return sb.toString();
    }

    private String parseExponentPart() {
        StringBuilder sb = new StringBuilder();
        if (buffer.hasNext() && (buffer.peek() == '+' || buffer.peek() == '-')) {
            sb.append(buffer.next());
        }
        while (buffer.hasNext() && Character.isDigit(buffer.peek())) {
            sb.append(buffer.next());
        }
        if (sb.isEmpty()) {
            throw new IllegalArgumentException("Invalid exponent at pos: " + buffer.getPos());
        }
        return sb.toString();
    }
}