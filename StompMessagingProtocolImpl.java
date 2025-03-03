package bgu.spl.net.impl;

import java.util.concurrent.ConcurrentLinkedQueue;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

public class StompMessagingProtocolImpl implements StompMessagingProtocol<String> {
    private boolean shouldTerminate = false;
    private int connectionId;
    private int msgID = 1;
    String errorMessage;
    Connections<String> connections;
    private String[] types = { "CONNECT", "SEND", "SUBSCRIBE", "UNSUBSCRIBE", "DISCONNECT" };

    @Override
    public void start(int connectionId, Connections<String> connections) {
        this.connections = connections;
        this.connectionId = connectionId;
    }

    @Override
    public void process(String message) {

        String[] lined = message.split("\n");
        lined = cleanArray(lined);// removes all the \n and spaces
        String receipt = receiptIfExists(lined);
        errorMessage = "ERROR\n" + receipt + "message: ";
        if (!checkIfType(lined[0])) {
            sendError("No known command found", message);
        } else {
            switch (lined[0]) {
                case "CONNECT":
                    if (!connections.isConnected(connectionId)) {
                        connectMe(lined, message);
                    } else {
                        sendError("User already logged in", message);
                    }
                    break;
                case "SUBSCRIBE":
                    String dest = null;
                    Integer id = null;
                    for (int i = 0; i < lined.length; i++) {
                        if (lined[i].startsWith("destination")) {
                            dest = lined[i].split(":")[1];
                        }
                        if (lined[i].startsWith("id")) {
                            id = Integer.valueOf(lined[i].split(":")[1]);
                        }
                    }
                    if (dest == null) {
                        sendError("No destination given", message);
                    }
                    if (id == null) {
                        sendError("No id given", message);
                    }
                    connections.subscribe(connectionId, dest, id);
                    sendReciept(lined);
                    break;
                case "UNSUBSCRIBE":
                    String channel = connections.getChannelById(connectionId + "," + lined[1].split(":")[1]);
                    if (channel == null) {
                        sendReciept(lined);
                        break;
                    }
                    connections.unsubscribe(connectionId, channel);
                    sendReciept(lined);
                    break;
                case "DISCONNECT":
                    if (lined[1].split(":")[0].equals("receipt")) {
                        connections.send(connectionId,
                                "RECEIPT\nreceipt-id:" + Integer.parseInt(lined[1].split(":")[1]) + "\n");
                        connections.disconnect(connectionId);
                    } else {
                        sendError("Invalid receipt request", message);
                    }
                    break;
                case "SEND":
                    // System.out.println(message);
                    String temp = null;
                    for (String s : lined) {
                        if (s.startsWith("destination")) {
                            temp = s;
                        }
                    }
                    if (temp == null) {
                        sendError("No destination given", message);
                        break;
                    }
                    String destinationChannel = temp.split(" ")[1];
                    String messageBody = "";
                    for (int i = 2; i < lined.length; i++) {
                        messageBody += lined[i] + '\n';
                    }
                    ConcurrentLinkedQueue<String> subscribers = connections.getChannels().get(destinationChannel);
                    if (subscribers == null || !isSubscribed(subscribers)) {// check sender is subscribed to the channel
                        sendError("Not subscribed to this topic", message);
                    }
                    else {
                        for (String i : subscribers) {// for each subscriberID in the channel
                            String[] ids = i.split(",");
                            int IDofAddressee = Integer.valueOf(ids[0]);
                            int subID = Integer.valueOf(ids[1]);
                            String msgPerIDmessageBody = "MESSAGE\nsubscription:" + subID + "\nmessage-id:" + msgID
                                    + "\ndestination:" + destinationChannel + "\n" + messageBody;
                            connections.send(IDofAddressee, msgPerIDmessageBody);
                        }
                    }
                    msgID++;
                    sendReciept(lined);
                    break;

            }
        }
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

    public String[] cleanArray(String[] msg) {// removes all the \n and spaces
        for (int i = 0; i < msg.length; i++) {
            msg[i] = msg[i].replace("\n", "").trim();
        }
        return msg;
    }

    public boolean checkIfType(String checkMe) {
        for (String s : types) {
            if (s.equals(checkMe)) {
                return true;
            }
        }
        return false;
    }

    public void connectMe(String[] linedMsg, String message) {
        String login = "";
        String passcode = "";
        boolean correctHost = false;
        boolean correctVersion = false;
        boolean correctLogin = false;
        boolean correctPasscode = false;
        int lineIndex = 1;
        while (lineIndex < linedMsg.length) {
            String[] currentLine = linedMsg[lineIndex].split(":");
            switch (currentLine[0]) {
                case "accept-version":
                    if (currentLine[1].equals("1.2")) {
                        correctVersion = true;
                    } else {
                        sendError("Wrong version", message);
                    }
                    break;
                case "host":
                    if (currentLine[1].equals("stomp.cs.bgu.ac.il")) {
                        correctHost = true;
                    } else {
                        sendError("Wrong host", message);
                    }
                    break;
                case "login":
                    correctLogin = true;
                    login = currentLine[1];
                    break;
                case "passcode":
                    if (!connections.isSignedUp(login)) {
                        correctPasscode = true;
                        passcode = currentLine[1];
                        break;
                    } else {
                        if (connections.getActiveUsers().containsValue(login)) {
                            sendError("User already logged in", message);
                            break;
                        }
                        if (connections.checkPasscode(login, currentLine[1])) {
                            correctPasscode = true;
                            passcode = currentLine[1];
                            break;
                        } else {
                            sendError("Wrong password", message);
                        }
                    }

                    break;
            }
            lineIndex++;
        }
        if (correctHost && correctLogin && correctPasscode && correctVersion) {
            connections.connect(login, passcode, connectionId);// saves login details
            // System.out.println("Sending connection confirmation...");
            connections.send(connectionId, "CONNECTED\nversion:1.2\n");
            sendReciept(linedMsg);
        } else {
            shouldTerminate = true;
        }
    }

    public void sendError(String error, String message) {
        errorMessage = errorMessage + error + "\n\nThe message:\n-----\n" + message + "\n-----\n";
        connections.send(connectionId, errorMessage);
        connections.disconnect(connectionId);
    }

    public String receiptIfExists(String[] lined) {
        for (String s : lined) {

            if (s.startsWith("receipt")) {

                String ret[] = s.split(":");
                return "receipt-id:" + ret[1] + "\n";
            }
        }
        return "";
    }

    public void sendReciept(String[] lined) {
        int ID;
        for (String s : lined) {

            if (s.startsWith("receipt")) {

                String ret[] = s.split(":");
                ID = Integer.parseInt(ret[1]);
                connections.send(connectionId, "RECEIPT\nreceipt-id:" + ID + "\n");
            }
        }
    }

    public boolean isSubscribed(ConcurrentLinkedQueue<String> subscribers) {
        for (String i : subscribers) {// for each subscriberID in the channel
            String[] ids = i.split(",");
            if (Integer.valueOf(ids[0]) == connectionId) {
                return true;
            }
        }
        return false;
    }
}
