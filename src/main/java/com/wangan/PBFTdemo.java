package com.wangan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PBFTdemo {
    private static final Logger log = LoggerFactory.getLogger(PBFTdemo.class);
    public static void main(String[] args) throws Exception{
        // 模拟 4 个节点，其中 Node0 是主节点
        for(int i = 0; i < Constant.TOTAL; i++){
            boolean primary = false;
            if(i == 0) primary = true;
            Node node = new Node(i, primary);
            NetwokContext.allNodes.add(node);
            Thread thread = new Thread(node);
            thread.setDaemon(true);
            thread.start();
        }
        log.info("===== case1: 正常共识 =====");
        case1();
        TimeUnit.SECONDS.sleep(5);
        printResult("case1", "提刀上洛", 0, 1); // 查询这条消息的共识结果

        log.info("===== case2: 主节点作恶 =====");
        case2();
        TimeUnit.SECONDS.sleep(5);
        printResult("case2-提刀上洛", "提刀上洛", 0, 2);
        printResult("case2-跑路", "跑路", 0, 2);
    }

    private static void printResult(String caseName, String digest, int viewNumber, int seqNumber) {
        String commitKey = String.format("%s|%d|%d|%s", Message.Type.COMMIT, viewNumber, seqNumber, digest);
        long committedCount = NetwokContext.allNodes.stream()
                .filter(node -> node.committedKeys.contains(commitKey))
                .count();
        if (committedCount >= Constant.QUORUM) {
            log.info("[{}] 共识达成，{} 个节点完成本地提交", caseName, committedCount);
        } else {
            log.info("[{}] 未能达成共识，仅 {} 个节点提交，未达到阈值 {}", caseName, committedCount, Constant.QUORUM);
        }
    }

    private static void case1(){
        String digest = "提刀上洛"; // 简化
        int senderId = 0; //主节点0
        Message prePrepare = new Message(Message.Type.PRE_PREPARE, 0, 1, digest, senderId);
        for (Node node : NetwokContext.allNodes) {
            if(node.id != senderId)
                node.receive(prePrepare);
        }
    }

    private static void case2(){
        int senderId = 0; //模拟主节点0在PRE_PREPARE阶段作恶
        NetwokContext.allNodes.get(1).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "提刀上洛", senderId));
        NetwokContext.allNodes.get(2).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "跑路", senderId));
        NetwokContext.allNodes.get(3).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "跑路", senderId));
    }
}