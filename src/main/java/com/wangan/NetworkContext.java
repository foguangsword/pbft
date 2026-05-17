package com.wangan;

import java.util.ArrayList;
import java.util.List;

/**
 * 模拟节点中保存的网络上下文
 * */
public class NetworkContext {
    public static List<Node> allNodes = new ArrayList<>();

    public static void broadcast(Message msg){
        for(Node node : allNodes){
            if(msg.senderId != node.id)
                node.receive(msg);
        }
    }
}
