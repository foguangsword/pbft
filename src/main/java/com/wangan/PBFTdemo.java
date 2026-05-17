package com.wangan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class PBFTdemo {
    private static final Logger log = LoggerFactory.getLogger(PBFTdemo.class);
    public static void main(String[] args) throws Exception{
        // 模拟 4 个节点，其中 Node0 是主节点
        for(int i = 0; i < Constant.TOTAL; i++){
            boolean primary = (i == Node.getPrimaryId(0));
            Node node = new Node(i, primary);
            NetworkContext.allNodes.add(node);
            Thread thread = new Thread(node);
            thread.setDaemon(true);
            thread.start();
        }
        log.info("===== case1: 正常共识 =====");
        case1();
        TimeUnit.SECONDS.sleep(2);
        printResult("case1", "提刀上洛", 0, 1);

        log.info("===== case2: 主节点作恶 =====");
        case2();
        TimeUnit.SECONDS.sleep(2);
        printResult("case2-提刀上洛", "提刀上洛", 0, 2);
        printResult("case2-跑路", "跑路", 0, 2);

        log.info("===== case3: 主节点宕机后 View Change =====");
        case3();
        printViewStatus("case3-after-viewchange");

        // 演示结束，强制停止所有后台定时器线程
        System.exit(0);
    }

    private static void printResult(String caseName, String digest, int viewNumber, int seqNumber) {
        String commitKey = String.format("%s|%d|%d|%s", Message.Type.COMMIT, viewNumber, seqNumber, digest);
        long committedCount = NetworkContext.allNodes.stream()
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
        int senderId = Node.getPrimaryId(0); // view=0 的主节点
        Message prePrepare = new Message(Message.Type.PRE_PREPARE, 0, 1, digest, senderId);
        for (Node node : NetworkContext.allNodes) {
            node.receive(prePrepare);
        }
    }

    private static void case2(){
        int senderId = Node.getPrimaryId(0); //模拟主节点0在PRE_PREPARE阶段作恶
        NetworkContext.allNodes.get(1).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "提刀上洛", senderId));
        NetworkContext.allNodes.get(2).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "跑路", senderId));
        NetworkContext.allNodes.get(3).receive(new Message(Message.Type.PRE_PREPARE, 0, 2, "跑路", senderId));
    }

    private static void case3() throws Exception {
        // 模拟主节点宕机：不给任何节点发 PRE_PREPARE，等待 view change 完成
        int stableView = 0;
        for (int i = 0; i < 20; i++) {
            TimeUnit.MILLISECONDS.sleep(500);
            int firstView = NetworkContext.allNodes.get(0).viewNumber;
            boolean allSame = NetworkContext.allNodes.stream()
                    .allMatch(n -> n.viewNumber == firstView && firstView > 0);
            if (allSame) {
                stableView = firstView;
                break;
            }
        }

        int newPrimary = Node.getPrimaryId(stableView);
        String digest = "清君侧";
        log.info("case3 向当前主节点{}发送请求，view={}", newPrimary, stableView);
        Message prePrepare = new Message(Message.Type.PRE_PREPARE, stableView, 3, digest, newPrimary);
        for (Node node : NetworkContext.allNodes) {
            node.receive(prePrepare);
        }
        TimeUnit.SECONDS.sleep(2);
        printResult("case3", digest, stableView, 3);
    }

    private static void printViewStatus(String caseName) {
        for (Node node : NetworkContext.allNodes) {
            log.info("[{}] 节点{} viewNumber={} primary={}", caseName, node.id, node.viewNumber, node.primary);
        }
    }
}