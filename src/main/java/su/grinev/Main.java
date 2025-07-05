package su.grinev;

import su.grinev.bson.BsonReader;
import su.grinev.bson.BsonWriter;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {

    public static byte[] loadJsonFile(String filePath) throws IOException {
        return Files.readAllBytes(Path.of(filePath));
    }

    public static ByteBuffer loadBsonFile(String filePath) throws IOException {
        byte[] data = Files.readAllBytes(Path.of(filePath));

        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);

        buffer.order(ByteOrder.LITTLE_ENDIAN); // важно для BSON!
        buffer.put(data);
        return buffer;
    }

    public static void main(String[] args) throws IOException {
//        byte[] largeJson = loadJsonFile("C:\\Users\\rgrin\\1MB.json");
//        List<Long> resultTokeinzer = new ArrayList<>();
//        List<Long> resultParser = new ArrayList<>();
//
//        for (int i = 0; i < 1000; i++) {
//            long delta = System.nanoTime();
//            Tokenizer tokenizer = new Tokenizer(largeJson);
//            List<Token> tokenList = tokenizer.tokenize();
//            delta = System.nanoTime() - delta;
//            resultTokeinzer.add(delta);
//            System.out.println("Tokenization: " + delta / 1000 + "us");
//
//            JsonParser parser = new JsonParser();
//
//            List<Object> jsonObject = new ArrayList<>();
//            delta = System.nanoTime();
//            jsonObject = parser.parseArray(tokenList);
//            delta = System.nanoTime() - delta;
//            resultParser.add(delta);
//            System.out.println("Parsing: " + delta / 1000 + "us");
//        }
//
//        System.out.println(resultTokeinzer.stream().sorted().toList().get(resultTokeinzer.size() / 2) / 1000);
//        System.out.println(resultParser.stream().sorted().toList().get(resultParser.size() / 2) / 1000);

        // Hex-представление BSON-документа (handcrafted):
        byte[] bson1 = new byte[] {
                0x45, 0x00, 0x00, 0x00, 0x02, 0x6b, 0x65, 0x79, 0x31, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75,
                0x65, 0x31, 0x00, 0x03, 0x6e, 0x65, 0x73, 0x74, 0x65, 0x64, 0x00, 0x27, 0x00, 0x00, 0x00, 0x02, 0x6b, 0x65,
                0x79, 0x32, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x32, 0x00, 0x02, 0x6b, 0x65, 0x79,
                0x33, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x33, 0x00, 0x00, 0x00
        };

        byte[] bson = new byte[] {
                // total document length = 100 (0x64)
                0x64, 0x00, 0x00, 0x00,

                // "name": "Alice"
                0x02, 'n', 'a', 'm', 'e', 0x00,
                0x06, 0x00, 0x00, 0x00, // length of "Alice" (5 + null)
                'A', 'l', 'i', 'c', 'e', 0x00,

                // "age": 30
                0x10, 'a', 'g', 'e', 0x00,
                0x1E, 0x00, 0x00, 0x00,

                // "address": { "city": "Springfield", "zip": 12345 }
                0x03, 'a', 'd', 'd', 'r', 'e', 's', 's', 0x00,
                0x20, 0x00, 0x00, 0x00,  // embedded document size = 36 (0x24)

                // "city": "Springfield"
                0x02, 'c', 'i', 't', 'y', 0x00,
                0x0C, 0x00, 0x00, 0x00,
                'S', 'p', 'r', 'i', 'n', 'g', 'f', 'i', 'e', 'l', 'd', 0x00,

                // "zip": 12345
                0x10, 'z', 'i', 'p', 0x00,
                0x39, 0x30, 0x00, 0x00, // 12345 little endian

                0x00, // end of "address" document

                // "tags": [ "user", "admin" ]
                0x04, 't', 'a', 'g', 's', 0x00,
                0x1A, 0x00, 0x00, 0x00,  // array doc length = 38 (0x26)

                // "0": "user"
                0x02, '0', 0x00,
                0x05, 0x00, 0x00, 0x00,
                'u', 's', 'e', 'r', 0x00,

                // "1": "admin"
                0x02, '1', 0x00,
                0x06, 0x00, 0x00, 0x00,
                'a', 'd', 'm', 'i', 'n', 0x00,

                0x00, // end of array

                0x00 // end of root document
        };



        Map m = Map.of(
                "0000", "1111",
                "Key1", "Value1",
                "Nested", Map.of("Key", "Value"),
                "Nested2", Map.of("Key1", "Value", "key2", "value", "key3", "value"),
                "Key2", "Value2",
                "Key3", "Value3"
        );

        BsonWriter bsonWriter = new BsonWriter();

        ByteBuffer b = bsonWriter.serialize(m);

        BsonReader bsonReader = new BsonReader();

        System.out.println(bsonReader.parseObject(b));

        List<Long> resultParser = new ArrayList<>();
        ByteBuffer buffer = loadBsonFile("C:\\Users\\rgrin\\large_test_multiple.bson");
        Map<String, Object> deserialized = Map.of();
        for (int i = 0; i < 10000; i++) {
                    long delta = System.nanoTime();
                    buffer.rewind();
                    deserialized = bsonReader.parseObject(buffer);
                    delta = System.nanoTime() - delta;
                   resultParser.add(delta);
        }

        System.out.println(deserialized);
        System.out.println(resultParser.stream().sorted().toList().get(resultParser.size() / 2) / 1000);
    }
}