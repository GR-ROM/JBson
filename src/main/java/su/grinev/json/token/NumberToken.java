package su.grinev.json.token;

public class NumberToken extends Token {

    private final Float aFloat;

    public NumberToken(Float aFloat) {
        super(TokenType.NUMBER);
        this.aFloat = aFloat;
    }

    public Float getNumber() {
        return aFloat;
    }

    public Float getaFloat() {
        return aFloat;
    }

    @Override
    public String toString() {
        return "NumberToken{" +
                "aFloat=" + aFloat +
                '}';
    }
}
