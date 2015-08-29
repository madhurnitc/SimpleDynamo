package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by maddy on 4/16/15.
 */
public class Message implements Serializable{
    public String senderID;
    public int msgType;
    public String key;
    public String value;
    public String ownerID;
    public String firstNext;
    public String secondNext;
    public HashMap<String,String> resultMap = new HashMap<>();

}
