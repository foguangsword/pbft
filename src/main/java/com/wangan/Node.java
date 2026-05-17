package com.wangan;

import com.alibaba.fastjson2.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * 模拟节点
 * */
public class Node implements Runnable{
    private static final Logger log = LoggerFactory.getLogger(Node.class);
    private static final long VIEW_CHANGE_TIMEOUT_MS = 5000;

    int id;
    public volatile boolean primary;
    volatile int viewNumber = 0;
    private volatile boolean viewChanging = false;
    BlockingQueue<Message> messagesQueue = new LinkedBlockingQueue<>();
    ConcurrentHashMap<String, Set<Integer>> msgCountMap = new ConcurrentHashMap<>(); //message key - set
    Set<String> committedKeys = ConcurrentHashMap.newKeySet();
    Set<String> commitBroadcasted = ConcurrentHashMap.newKeySet();
    ConcurrentHashMap<Integer, Set<Integer>> viewChangeVotes = new ConcurrentHashMap<>();
    Set<Integer> newViewBroadcasted = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService viewChangeTimer = Executors.newSingleThreadScheduledExecutor();
    private volatile ScheduledFuture<?> timeoutFuture;

    public Node (int id, boolean primary){
        this.id = id;
        this.primary = primary;
        resetViewChangeTimeout();
    }

    // 获取指定 view 的主节点 ID
    static int getPrimaryId(int viewNumber) {
        return Math.abs(viewNumber) % Constant.TOTAL;
    }

    void resetViewChangeTimeout() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
        timeoutFuture = viewChangeTimer.schedule(this::onViewChangeTimeout, VIEW_CHANGE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    }

    void onViewChangeTimeout() {
        if (viewChanging) {
            return;
        }
        log.info("节点{} view change 超时，当前view={}", this.id, this.viewNumber);
        viewChanging = true;

        int newView = this.viewNumber + 1;
        Message viewChangeMsg = new Message(Message.Type.VIEW_CHANGE, newView, -1, "", this.id);
        NetworkContext.broadcast(viewChangeMsg);

        viewChangeVotes.computeIfAbsent(newView, k -> new ConcurrentSkipListSet<>()).add(this.id);
        checkAndBroadcastNewView(newView);
    }

    void checkAndBroadcastNewView(int newView) {
        Set<Integer> voters = viewChangeVotes.get(newView);
        if (voters != null && voters.size() >= Constant.QUORUM) {
            if (this.id == getPrimaryId(newView)) {
                if (!newViewBroadcasted.add(newView)) {
                    return;
                }
                log.info("节点{}成为新主节点，广播NEW_VIEW，view={}", this.id, newView);
                this.viewNumber = newView;
                this.primary = true;
                Message newViewMsg = new Message(Message.Type.NEW_VIEW, newView, -1, "", this.id);
                NetworkContext.broadcast(newViewMsg);
                viewChanging = false;
                resetViewChangeTimeout();
            }
        }
    }

    //接收消息，仅入队，计票放到 handle 阶段以保证日志顺序与逻辑一致
    void receive(Message message) {
        messagesQueue.add(message);
    }

    void handleMessage(Message msg){
        switch (msg.type){
            case PRE_PREPARE:
            case PREPARE:
            case COMMIT:
                if (msg.viewNumber != this.viewNumber) return;
                break;
            case VIEW_CHANGE:
            case NEW_VIEW:
                break;
        }
        switch (msg.type){
            case PRE_PREPARE: handlePrePrepare(msg); break;
            case PREPARE: handlePrepare(msg); break;
            case COMMIT: handleCommit(msg); break;
            case VIEW_CHANGE: handleViewChange(msg); break;
            case NEW_VIEW: handleNewView(msg); break;
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
        resetViewChangeTimeout();
    }

    // 节点收到prepare消息，判断一致的消息数量是否达到2f+1，是则广播commit消息
    void handlePrepare(Message msg) {
        log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg));
        String key = msg.key();

        // 计票：在处理阶段计数，保证日志与逻辑顺序一致
        msgCountMap.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(msg.senderId);

        if (isMessageTrue(msg) && commitBroadcasted.add(key)) {
            log.info(this.id + "节点收到PREPARE消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，广播COMMIT消息");
            Message commitMsg = new Message(Message.Type.COMMIT, msg.viewNumber, msg.seqNumber, msg.digest, this.id);
            NetworkContext.broadcast(commitMsg);
            msgCountMap.computeIfAbsent(commitMsg.key(), k -> new ConcurrentSkipListSet<>()).add(this.id);
            log.info(this.id + "节点收到COMMIT消息（本节点广播）" + JSON.toJSONString(commitMsg));
        }
    }

    //节点收到commit消息，判断一致的消息数量是否达到2f+1，是则最终提交
    void handleCommit(Message msg){
        log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg));
        String key = msg.key();

        // 计票：在处理阶段计数，保证日志与逻辑顺序一致
        msgCountMap.computeIfAbsent(key, k -> new ConcurrentSkipListSet<>()).add(msg.senderId);

        // 安全校验：只提交自己已经参与过 COMMIT 的提案
        Set<Integer> commitSenders = msgCountMap.get(key);
        if (commitSenders == null || !commitSenders.contains(this.id)) {
            return;
        }

        if (isMessageTrue(msg) && committedKeys.add(key)) {
            log.info(this.id + "节点收到COMMIT消息" + JSON.toJSONString(msg) + "后，已收到满足数量的一致消息，进行本地提交执行");
        }
    }


    void handleViewChange(Message msg) {
        if (msg.viewNumber <= this.viewNumber) {
            return;
        }

        log.info(this.id + "节点收到VIEW_CHANGE消息" + JSON.toJSONString(msg));
        viewChangeVotes.computeIfAbsent(msg.viewNumber, k -> new ConcurrentSkipListSet<>()).add(msg.senderId);
        checkAndBroadcastNewView(msg.viewNumber);
    }

    void handleNewView(Message msg) {
        if (msg.senderId != getPrimaryId(msg.viewNumber)) {
            log.warn("节点{}拒绝非法NEW_VIEW: view={} 的主节点应为{}, 实际sender={}",
                    this.id, msg.viewNumber, getPrimaryId(msg.viewNumber), msg.senderId);
            return;
        }

        log.info(this.id + "节点收到NEW_VIEW消息" + JSON.toJSONString(msg));
        this.viewNumber = msg.viewNumber;
        this.primary = (this.id == getPrimaryId(msg.viewNumber));
        viewChanging = false;
        resetViewChangeTimeout();
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
