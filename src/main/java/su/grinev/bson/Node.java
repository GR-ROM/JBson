package su.grinev.bson;

import java.util.LinkedList;
import java.util.List;

public class Node {

    public int lengthPos;
    public int length;
    public Node parent;
    public List<Node> nested;

    public Node(Node parent) {
        length = 0;
        nested = new LinkedList<>();
        this.parent = parent;
    }

}
