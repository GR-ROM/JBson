package su.grinev;

import su.grinev.bson.BsonReader;
import su.grinev.bson.BsonWriter;
import su.grinev.test.VpnPacket;
import su.grinev.test.VpnRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

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

        Map<String, Object> doc1 = Map.of("a", 1);
        Map<String, Object> doc2 = Map.of("b", 2);
        List<Object> list = Arrays.asList(doc1, doc2);
        Map<String, Object> doc = Map.of("list", list);

        BsonWriter writer = new BsonWriter();
        ByteBuffer buffer = writer.serialize(doc);
        BsonReader reader = new BsonReader();

        Map<String, Object> document = reader.deserialize(buffer);
        System.out.println(document);

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

        byte[] bson1 = new byte[] {
                0x45, 0x00, 0x00, 0x00, 0x02, 0x6b, 0x65, 0x79, 0x31, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75,
                0x65, 0x31, 0x00, 0x03, 0x6e, 0x65, 0x73, 0x74, 0x65, 0x64, 0x00, 0x27, 0x00, 0x00, 0x00, 0x02, 0x6b, 0x65,
                0x79, 0x32, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x32, 0x00, 0x02, 0x6b, 0x65, 0x79,
                0x33, 0x00, 0x07, 0x00, 0x00, 0x00, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x33, 0x00, 0x00, 0x00
        };

        Binder binder = new Binder();
        BsonWriter bsonWriter = new BsonWriter();
        BsonReader bsonReader = new BsonReader();

        byte[] packet = new byte[128 * 1024];

        List<String> test = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < 1000; i++) {
            test.add(Integer.toString(random.nextInt()));
        }

        VpnRequest<VpnPacket> request = VpnRequest.<VpnPacket>builder()
                .ver("0.1")
                .type(VpnPacket.class.getTypeName())
                .opts(Map.of(
                        "srcCountry", "kz",
                        "dstCounty", "ru"
                        ))
                .reserved(test)
                .data(VpnPacket.builder()
                        .encoding("RAW")
                        .packet(packet)
                        .build())
                .build();

        VpnRequest<VpnPacket> request1 = null;
        ByteBuffer b = null;
        List<Long> serializationTime = new ArrayList<>();
        List<Long> deserializationTime = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {

            long delta = System.nanoTime();

            Map<String, Object> documentMap = binder.unbind(request);
            b = bsonWriter.serialize(documentMap);

            serializationTime.add((System.nanoTime() - delta) / 1000);

            delta = System.nanoTime();

            Map<String, Object> deserialized = bsonReader.deserialize(b);
            request1 = binder.bind(VpnRequest.class, deserialized);

            deserializationTime.add((System.nanoTime() - delta) / 1000);
        }
        System.out.println(Arrays.toString(b.array()));
        System.out.println(request1);

        List<Long> sortedSerialization = serializationTime.stream().sorted().toList();
        List<Long> sortedDeserialization = deserializationTime.stream().sorted().toList();

        System.out.println("Serialization median time: %sus".formatted(Long.toString(sortedSerialization.get(sortedSerialization.size() / 2))));
        System.out.println("Deserialization median time: %sus".formatted(Long.toString(sortedDeserialization.get(sortedDeserialization.size() / 2))));


    }
}