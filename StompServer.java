package bgu.spl.net.impl.stomp;

import bgu.spl.net.impl.StompMessagingProtocolImpl;
import bgu.spl.net.impl.StompMessageEncoderDecoder;
import bgu.spl.net.srv.Server;

public class StompServer {

    public static void main(String[] args) {
        if(args[1].equals("tpc")){
            System.out.println("You chose tpc");
            Server.threadPerClient(
                7777, 
                ()-> new StompMessagingProtocolImpl(), 
                ()-> new StompMessageEncoderDecoder()).serve();
        }
        else if(args[1].equals("reactor")){
            System.out.println("You chose reactor");
            Server.reactor(Runtime.getRuntime().availableProcessors(),
            7777, 
            ()-> new StompMessagingProtocolImpl(), 
            ()-> new StompMessageEncoderDecoder()).serve();
        }
        else{
            System.out.println("You chose an invalid type of server. Please try again with tpc/reactor");
        }
    }
}
