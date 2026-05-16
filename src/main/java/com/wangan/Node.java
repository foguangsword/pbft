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
    ConcurrentHashMap<String, Boolean> msgBroadMap = new ConcurrentHashMap<>(); //message key - bool
    Set<String> committedKeys = ConcurrentHashMap.newKeySet();

    public Node (int id, boolean primary){
        this.id = id;
        this.primary = primary;
    }

    //接收消息
    void receive(Message message) {
        msgCountMap.computeIfAbsent(message.key(), k -> new ConcurrentSkipListSet<>()).add(message.senderId);
        msgBroadMap.putIfAbsent(message.key(), false);
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

    // 节点收PRE_PREPARE消息，到网络中广播 prepare消息
    void handlePrePrepare(Message msg) {
        log.info(this.id + "节点收到PRE-PREPARE消息" + JSON.toJSONString(msg));
        Message prepareMsg = new Message(Message.Type.PREPARE, msg.viewNumber, msg.seqNumber, msg.digest, this.id);
        NetwokContext.broadcast(prepareMsg);
        msgCountMap.computeIfAbsent(prepareMsg.key(), k -> new ConcurrentSkipListSet<>()).add(this.id); //自己记prepare消息
        msgBroadMap.put(msg.key(), true); //PRE-PREPARE消息已做了广播处理，广播为PREPARE消息。PRE-PREPARE默认为真
    }

    // 节点收到prepare消息，判断一致的消息数量是否达到2n+1，是则广播commit消息
    void handlePrepare(Message msg) {
        log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg));
        String key = msg.key();
        if(isMessageTrue(msg) && !msgBroadMap.get(key)){ //没广播过
            log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，广播COMMIT消息");
            Message commitMsg = new Message(Message.Type.COMMIT, msg.viewNumber, msg.seqNumber, msg.digest, this.id);
            NetwokContext.broadcast(commitMsg);
            msgCountMap.computeIfAbsent(commitMsg.key(), k -> new ConcurrentSkipListSet<>()).add(this.id); //自己记commit消息
            msgBroadMap.put(key, true);
        }
    }

    //节点收到commit消息，判断一致的消息数量是否达到2n+1，是则最终提交
    void handleCommit(Message msg){
        log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg));
        String key = msg.key();
        if(isMessageTrue(msg) && !msgBroadMap.get(key)) {
            log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，进行本地提交执行");
            committedKeys.add(key); //记录已提交
            msgBroadMap.put(key, true);
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
