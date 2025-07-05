package su.grinev.json.token;

public class Token {

    private TokenType type;

    public Token(TokenType tokenType) {
        this.type = tokenType;
    }

    public TokenType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "Token{" +
                "type=" + type +
                '}';
    }
}
