package su.grinev.json.token;

public class StringParser {

    private final Buffer buffer;
    private final StringPool stringPool = new StringPool();

    public StringParser(Buffer buffer) {
        this.buffer = buffer;
    }

    public StringToken parseString() {
        buffer.next();
        StringBuilder sb = new StringBuilder();
        int startPos = buffer.getPos();
        boolean hasEscapeSequence = false;
        int count = 0;
        while (true) {
            long data = buffer.getLong();
            long maskedEscape = escapeMask(data);
            long maskedQuote = quoteMask(data);

            if (maskedEscape != 0) {
                hasEscapeSequence = true;
                break;
            }

            if (maskedQuote != 0) {
                int position = getPosition(maskedQuote);
                count += position;
                buffer.setPost(buffer.getPos() + position);
                break;
            }
            count += 8;
            buffer.setPost(buffer.getPos() + 8);
        }


        if (!hasEscapeSequence) {
            buffer.next();
            return new StringToken(buffer.getString(startPos, count));
        } else {
            buffer.setPost(startPos);
            char c;
            while (true) {
                if (!buffer.hasNext()) {
                    throw new IllegalArgumentException("Unexpected end of input in string");
                }

                c = buffer.next();
                if (c == '"') break;
                if (c == '\\') {
                    if (!buffer.hasNext()) {
                        throw new IllegalArgumentException("Unexpected end after escape at pos: " + buffer.getPos());
                    }
                    char esc = buffer.next();
                    sb.append(switch (esc) {
                        case '"' -> '"';
                        case '\\' -> '\\';
                        case '/' -> '/';
                        case 'b' -> '\b';
                        case 'f' -> '\f';
                        case 'n' -> '\n';
                        case 'r' -> '\r';
                        case 't' -> '\t';
                        case 'u' -> parseUnicode();
                        default -> throw new IllegalArgumentException("Invalid escape character: \\" + esc);
                    });
                } else {
                    sb.append(c);
                }
            }
        }
        return new StringToken(sb.toString());
    }

    private static int getPosition(long masked) {
        for (int i = 0; i < 8; i++) {
            if ((masked & 0x8000000000000000L) != 0) {
                return i;
            }
            masked <<= 8;
        }
        return 0;
    }

    public static long quoteMask(long word) {
        long cmp = word ^ 0x2222222222222222L;
        return  ((cmp - 0x0101010101010101L) & ~cmp & 0x8080808080808080L);
    }

    public static long escapeMask(long word) {
        long cmp = word ^ 0x5C5C5C5C5C5C5C5CL;
        return  ((cmp - 0x0101010101010101L) & ~cmp & 0x8080808080808080L);
    }

    private char parseUnicode() {
        if (buffer.getPos() + 4 > buffer.size()) {
            throw new IllegalArgumentException("Invalid unicode escape at pos: " + buffer.getPos());
        }

        StringBuilder hex = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            hex.append(buffer.next());
        }
        return (char) Integer.parseInt(hex.toString(), 16);
    }
}
