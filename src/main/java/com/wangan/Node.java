package com.wangan;

import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 模拟节点
 * */
public class Node implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    int id;
    public boolean primary;
    BlockingQueue<Message> messagesQueue = new LinkedBlockingQueue<>();
    ConcurrentHashMap<String, Set<Integer>> msgCountMap = new ConcurrentHashMap<>(); //message key - set
    ConcurrentHashMap<String, Boolean> msgActionMap = new ConcurrentHashMap<>(); //message key - bool
    Set<String> committedKeys = ConcurrentHashMap.newKeySet();

    public Node (int id, boolean primary){
        this.id = id;
        this.primary = primary;
    }

    // 获取指定 view 的主节点 ID
    static int getPrimaryId(int viewNumber) {
        return Math.abs(viewNumber) % Constant.TOTAL;
    }

    //接收消息，仅入队，计票放到 handle 阶段以保证日志顺序与逻辑一致
    void receive(Message message) {
        messagesQueue.add(message);
    }

    void handleMessage(Message msg){
        switch (msg.type){
            case PRE_PREPARE: handlePrePrepare(msg); break;
            case PREPARE: handlePrepare(msg); break;
            case COMMIT: handleCommit(msg); break;
        }
    }

    private boolean isMessageTrue(Message msg){
        String key = msg.key();
        if(null != msgCountMap.get(key)){
            if(msgCountMap.get(key).size() >= Constant.QUORUM) return true;
        }
        return false;
    }

    // 节点收PRE_PREPARE消息，验证主节点身份后到网络中广播 prepare消息
    void handlePrePrepare(Message msg) {
        if (msg.senderId != getPrimaryId(msg.viewNumber)) {
            log.warn("节点{}拒绝非法PRE_PREPARE: view={} 的主节点应为{}, 实际sender={}",
                    this.id, msg.viewNumber, getPrimaryId(msg.viewNumber), msg.senderId);
            return;
        }

        log.info(this.id + "节点收到PRE-PREPARE消息" + JSON.toJSONString(msg));
        Message prepareMsg = new Message(Message.Type.PREPARE, msg.viewNumber, msg.seqNumber, msg.digest, this.id);
        NetworkContext.broadcast(prepareMsg);
        msgCountMap.computeIfAbsent(prepareMsg.key(), k -> new ConcurrentSkipListSet<>()).add(this.id);
        msgActionMap.putIfAbsent(prepareMsg.key(), false);
        msgActionMap.put(msg.key(), true);
    }

    // 节点收到prepare消息，判断一致的消息数量是否达到2f+1，是则广播commit消息
    void handlePrepare(Message msg) {
        log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg));
        String key = msg.key();

        // 计票：在处理阶段计数，保证日志与逻辑顺序一致
        msgCountMap.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(msg.senderId);
        msgActionMap.putIfAbsent(key, false);

        if (isMessageTrue(msg) && Boolean.FALSE.equals(msgActionMap.get(key))) {
            log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，广播COMMIT消息");
            Message commitMsg = new Message(Message.Type.COMMIT, msg.viewNumber, msg.seqNumber, msg.digest, this.id);
            NetworkContext.broadcast(commitMsg);
            msgCountMap.computeIfAbsent(commitMsg.key(), k -> new ConcurrentSkipListSet<>()).add(this.id);
            msgActionMap.putIfAbsent(commitMsg.key(), false);
            msgActionMap.put(key, true);
        }
    }

    //节点收到commit消息，判断一致的消息数量是否达到2f+1，是则最终提交
    void handleCommit(Message msg){
        log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg));
        String key = msg.key();

        // 计票：在处理阶段计数，保证日志与逻辑顺序一致
        msgCountMap.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(msg.senderId);
        msgActionMap.putIfAbsent(key, false);

        // 安全校验：只提交自己已经参与过 COMMIT 的提案
        Set<Integer> commitSenders = msgCountMap.get(key);
        if (commitSenders == null || !commitSenders.contains(this.id)) {
            return;
        }

        if (isMessageTrue(msg) && Boolean.FALSE.equals(msgActionMap.get(key))) {
            log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，进行本地提交执行");
            committedKeys.add(key); //记录已提交
            msgActionMap.put(key, true);
        }
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()){
            try {
                handleMessage(messagesQueue.take());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
