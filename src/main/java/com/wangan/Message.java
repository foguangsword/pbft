package com.wangan;

import lombok.Data;

@Data
public class Message {
    public enum Type {PRE_PREPARE, PREPARE, COMMIT};
    Type type; //消息类型
    int viewNumber; //任期，主节点不换任期不变
    int seqNumber; //消息在某任期下的序列号
    String digest; //消息摘要
    int senderId; //发送者ID，消息是谁发的

    public Message(Type type, int viewNumber, int seqNumber, String digest, int senderId){
        this.type  = type;
        this.viewNumber = viewNumber;
        this.seqNumber = seqNumber;
        this.digest = digest;
        this.senderId = senderId;
    }

    public String key(){
        return String.format("%s|%d|%d|%s", this.type , this.viewNumber, this.seqNumber, this.digest);
    }
}
