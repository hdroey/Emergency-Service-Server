package bgu.spl.net.srv;

// import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public interface Connections<T> {

    boolean send(int connectionId, T msg);

    void send(int IDofAddressee, String channel, T msg);

    void disconnect(int connectionId);

    void addHandler(int id, ConnectionHandler<T> handler);
    
    void connect(String username, String passcode, int id);
    
    public boolean isConnected(int id);

    public boolean isSignedUp(String username);

    public boolean checkPasscode(String username, String passcode);

    public void subscribe(int connectionId,String channel,int id);

    public void unsubscribe(int connectionId,String channel);

    // public Integer getIdByChannelId(Integer channelId);

    public String getChannelById(String connectionchannel);

    public ConcurrentHashMap<String,ConcurrentLinkedQueue<String>> getChannels();

    public ConcurrentHashMap<String,Integer> getidByChannel();

    public ConcurrentHashMap<Integer,String> getActiveUsers();
}
