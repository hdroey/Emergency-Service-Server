package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.ConnectionsImpl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    private final StompMessagingProtocol<T> protocol; //protocol- how we respond to messages
    private final MessageEncoderDecoder<T> encdec; //the encoding decoding policty for upcoming bytes 
    private final Socket sock; //socket
    private final int clientID;
    private BufferedInputStream in; //bytes from client going here
    private BufferedOutputStream out; //bytes from the server going here
    private volatile boolean connected = true;

    public BlockingConnectionHandler(Socket sock, MessageEncoderDecoder<T> reader, StompMessagingProtocol<T> protocol,int clientID) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.clientID=clientID;
        protocol.start(clientID, ConnectionsImpl.getInstance());
    }

    @Override
    public void run() {
        try (Socket sock = this.sock) { //just for automatic closing
            int read;

            in = new BufferedInputStream(sock.getInputStream());
            out = new BufferedOutputStream(sock.getOutputStream());

            while (!protocol.shouldTerminate() && connected && (read = in.read()) >= 0) {
                T nextMessage = encdec.decodeNextByte((byte) read);
                if (nextMessage != null) {
                    protocol.process(nextMessage);//moved 'response' to send method
                    // if (response != null) {
                    //     out.write(encdec.encode(response));
                    //     out.flush();
                    // }
                }
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }


    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
    }


    @Override
    public void send(T msg) {
        try{
        out.write(encdec.encode(msg));
        out.flush();
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }
    public int getID(){
        return clientID;
    }

}
