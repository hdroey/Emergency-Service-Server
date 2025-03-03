package bgu.spl.net.impl;

import java.util.concurrent.*;
// import java.io.IOException;
import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;

public class ConnectionsImpl<T> implements Connections<T> {
    private static class ConnectionsSingleton {
        private static final ConnectionsImpl<?> Singleton = new ConnectionsImpl<>();
    }

    @SuppressWarnings("unchecked")
    public static <T> ConnectionsImpl<T> getInstance() {
        return (ConnectionsImpl<T>) ConnectionsSingleton.Singleton;
    }

    private ConnectionsImpl() {
        IDS = new ConcurrentHashMap<>();
        channels = new ConcurrentHashMap<>();
        UsernametoPasscode = new ConcurrentHashMap<>();
        activeIDS = new ConcurrentLinkedQueue<>();
        activeUsersByID = new ConcurrentHashMap<>();
        idByChannel = new ConcurrentHashMap<>();
        originalID = new ConcurrentHashMap<>();
    }

    ConcurrentHashMap<Integer, ConnectionHandler<T>> IDS;
    ConcurrentLinkedQueue<Integer> activeIDS;
    ConcurrentHashMap<Integer, String> activeUsersByID;
    ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> channels;// "Originalid,subscriptionId"
    ConcurrentHashMap<String, String> UsernametoPasscode;
    ConcurrentHashMap<String/* ChannelName:OriginalID */, String> originalID;// return id in
                                                                             // channel|value:Originalid,subscriptionId
    ConcurrentHashMap<String/* ChannelName:connectionId,IdInChannel */, Integer> idByChannel;// return original ID

    public boolean send(int connectionId, T msg) {

        try {
            IDS.get(connectionId).send(msg);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public void send(int IDofAddressee, String channel, T msg) {// do we even use this for something?
        ConnectionHandler<T> handler = IDS.get(IDofAddressee);
        if (handler != null) {
            handler.send(msg);
        }
    }

    public void disconnect(int connectionId) {
        // ConnectionHandler<T> handler = IDS.get(connectionId);
        // try {
        IDS.remove(connectionId);
        activeIDS.remove(connectionId);
        activeUsersByID.remove(connectionId);
        for (String s : channels.keySet()) {
            idByChannel.remove(s + ":" + originalID.get(s + ":" + connectionId));
            channels.get(s).remove(originalID.get(s + ":" + connectionId));
            originalID.remove(s + ":" + connectionId);
        }
        // handler.close();
        // }
        // catch (IOException e) {
        // e.printStackTrace();
        // }
    }

    public void addHandler(int connectionId, ConnectionHandler<T> handler) {
        IDS.putIfAbsent(connectionId, handler);
    }

    public boolean isConnected(int id) {
        // System.out.println("Checking id..");
        if (activeIDS.contains(id)) {
            return true;
        }
        activeIDS.add(id);
        return false;
    }

    public boolean isSignedUp(String username) {
        if (UsernametoPasscode.containsKey(username)) {
            return true;
        }
        return false;
    }

    public boolean checkPasscode(String username, String passcode) {
        if (UsernametoPasscode.get(username).equals(passcode)) {
            return true;
        }
        return false;
    }

    public void connect(String username, String passcode, int id) {
        UsernametoPasscode.putIfAbsent(username, passcode);
        activeUsersByID.put(id, username);
    }

    // public Integer getIdByChannelId(Integer channelId){ // ????
    // for(String channel:channels.keySet()){
    // if(channels.get(channel).contains(channelId)){
    // return idByChannel.get(channel+":"+originalID);
    // }
    // }
    // return null;
    // }
    public String getChannelById(String connectionchannel) {
        for (String channel : channels.keySet()) {
            if (channels.get(channel).contains(connectionchannel)) {
                return channel;
            }
        }
        return null;
    }

    public void subscribe(int connectionId, String channel, int id) {
        idByChannel.put(channel + ":" + connectionId + "," + id, connectionId);
        originalID.put(channel + ":" + connectionId, connectionId + "," + id);
        channels.putIfAbsent(channel, new ConcurrentLinkedQueue<String>());
        channels.get(channel).add(connectionId + "," + id);
        ConcurrentHashMap<Integer, String> IDofChannel = new ConcurrentHashMap<>();
        IDofChannel.put(id, channel);
    }

    public void unsubscribe(int connectionId, String channel) {
        String channelId = originalID.get(channel + ":" + connectionId);
        channels.get(channel).remove(channelId);
        idByChannel.remove(channel + ":" + channelId);
        originalID.remove(channel + ":" + connectionId);
    }

    public ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> getChannels() {
        return channels;
    }

    public ConcurrentHashMap<String, Integer> getidByChannel() {
        return idByChannel;
    }

    public ConcurrentHashMap<Integer, String> getActiveUsers() {
        return activeUsersByID;
    }
}
