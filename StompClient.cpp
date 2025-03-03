#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <ConnectionHandler.h>
#include <StompProtocol.h>
#include <event.h>
// #include <event>
#include <thread>
#include <mutex>
#include <unordered_map>
#include "boost/algorithm/string.hpp"
// #include "StompProtocol.h"
using namespace std;

int ReceiptCounter = (1);
int ClientCounter = (0);
std::string username;
bool isConnected = false;
std::mutex SubscriptionIdToChannelLock;
std::mutex idToFrameMapLock;
std::mutex ReceiptIdToSubscriptionIdLock;
std::mutex UserChannelEventMapLock;
std::unordered_map<int, string> SubscriptionIdToChannel ;                 // subs id to channel name
std::unordered_map<int, string> idToFrameMap ;                            // receipt id to Frame name
std::unordered_map<int, int> ReceiptIdToSubscriptionId ;                     // rec id to sub id
std::unordered_map<string, vector<Event>> UserChannelEventMap; // mapped channel,user ->event vector
int main(int argc, char *argv[])
{
    StompProtocol foo;
    return foo.main(argc,argv);
}
int StompProtocol::main(int argc, char *argv[])
{
    cout << "Client started" << std::endl;
    ConnectionHandler ch("foo",0);
    while (!isConnected)//check initial login before starting the threads
    {
        vector<string> details;
        while (true)
        {
            
            const short bufsize = 1024;
            char buf[bufsize];
            std::cin.getline(buf, bufsize);
            std::string line(buf);
            vector<string> parsedCommand;
            boost::split(parsedCommand, line, boost::is_any_of(" "));
            if (parsedCommand[0] != "login")
            {
                cout << "Attempt to log in correctly..." << std::endl;
            }
            else
            {
                details = parsedCommand;
                break;
            }
        }
        vector<string> hostport;
        boost::split(hostport, details[1], boost::is_any_of(":"));
        short port = atoi((hostport[1]).c_str());
        ch.setHost(hostport[0]);
        ch.setPort(port);
        // ConnectionHandler ch(hostport[0], port);
        if (!ch.connect())
        {
            std::cerr << "Cannot connect to " << hostport[0] << ":" << port << std::endl;
            break;
        }
        string sendLoginFrame = "CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\nlogin:" + details[2] + "\npasscode:" + details[3]/* + "\0"*/;
        ch.sendLine(sendLoginFrame);
        username = details[2];
        isConnected = checkLoginResponse(std::ref(ch));
        break;
    }
    std::thread read_Console_Thread(&StompProtocol::readConsoleTask,this,std::ref(ch));
    std::thread read_Socket_Thread(&StompProtocol::readSocketTask,this,std::ref(ch));
    read_Console_Thread.join();
    read_Socket_Thread.join();
    return 0;
}
void StompProtocol::readConsoleTask(ConnectionHandler &ch)
{
    while (true)
    {
        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        std::string line(buf);
        // int len = line.length();
        // if (!(ch).sendLine(line))
        // {
        //     std::cout << "Disconnected. Exiting...\n"
        //               << std::endl;
        //     break;
        // }
        vector<string> parsedCommand = StompProtocol::splitString(line, " ");
        if ((parsedCommand).at(0) == "login")
        {
            if(isConnected){
                std::cout << "Client already logged in" << std::endl;
            } else {
                string s=StompProtocol::createLoginFrame(line);
                ch.connect();
                ch.sendLine(s);
                isConnected = checkLoginResponse(std::ref(ch));
                if(!isConnected){
                    ch.close();
                }
            }
        }
        if(isConnected){
        if ((parsedCommand).at(0) == "join")
        {
            string s=StompProtocol::createJoinFrame(line);
            ch.sendLine(s);
        }
        if ((parsedCommand).at(0) == "exit")
        {
            string s=StompProtocol::createExitFrame(line);
            if(s.empty()){
                continue;
            }
            else{
                ch.sendLine(s);
            }
        }
        if ((parsedCommand).at(0) == "report")
        {
            vector<string> reportEventVec = StompProtocol::createReportFrame(parsedCommand.at(1));// fix to generic path
            for (string s : reportEventVec)
            {
                ch.sendLine(s);
            }
        }
        if ((parsedCommand).at(0) == "summary")
        {
            StompProtocol::createSummaryFrame(parsedCommand.at(1),parsedCommand.at(2),(parsedCommand).at(3));
        }
        if ((parsedCommand).at(0) == "logout")
        {
            string s=StompProtocol::createLogoutFrame(line);
            //cout << s;
            ch.sendLine(s);
            // should terminate here?
        }
        }
        else{
            std::cout << "First of all, you must attempt to login correctly..." << std::endl;
        }
    }
}
void StompProtocol::readSocketTask(ConnectionHandler &ch)
{
    while(true){
    while (isConnected)
    {
        std::string answer;
        if (isConnected && !(ch).getLine(answer))
        {
            std::cout << "Disconnected. Exiting...\n"
                      << std::endl;
            break;
        }
        int len = answer.length();
        answer.resize(len - 1);
        vector<string> vec = StompProtocol::splitString(answer, "\n");
        if ((vec).at(0) == "CONNECTED")
        {
            cout << "login successful";
            isConnected = true;
        }
        if ((vec).at(0) == "MESSAGE")
        {
            handleMessageFrame(answer);
        }
        if ((vec).at(0) == "ERROR")
        {
            isConnected = false;//?????
            ch.close();
            for(string s : vec){
            std::cout << s << std::endl;
            }
        }
        if ((vec).at(0) == "RECEIPT")
        {
            vector<string> extractReceiptId = splitString((vec).at(1), ":");
            if (getFromFrameMap(atoi((extractReceiptId).at(1).c_str())) == "SUBSCRIBE")
            {
                int subscriptionID = getFromReceiptIdSubscriptionId(atoi((extractReceiptId).at(1).c_str()));
                std::string channelName = getFromChannelMap(subscriptionID);
                std::cout << "Joined Channel " + channelName << std::endl;
            }
            if (getFromFrameMap(atoi((extractReceiptId).at(1).c_str())) == "DISCONNECT")
            {
                std::cout << "Logging out..." << std::endl;
                isConnected = false;
                ch.close();
            }
            if (getFromFrameMap(atoi((extractReceiptId).at(1).c_str())) == "UNSUBSCRIBE")
            {
                int subscriptionID = getFromReceiptIdSubscriptionId(atoi((extractReceiptId).at(1).c_str()));
                std::string channelName = getFromChannelMap(subscriptionID);
                std::cout << "Exited Channel " + channelName << std::endl;            }
        }
        
    }
    }
}
void StompProtocol::handleMessageFrame(const string& message)
{

    vector<string> parsedMessage = splitString(message, "\n");
    string user;
    // string city;
    // string date;
    // string name;
    // string summary;
    string channel;
    for (string s : parsedMessage)
    {
        if (startsWith(s,"user"))
        {
            vector<string> tmp = splitString(s, " ");
            user = tmp.at(1);
            
        }
        /* if(s.starts_with("city")){
                vector<string>* tmp=parsedMessage(s,":");
                city=tmp->at(1);
                delete tmp;
            }
            if(s.starts_with("date")){
                vector<string>* tmp=parsedMessage(s,':');
                date=tmp->at(1);
                delete tmp;
            }
            if(s.starts_with("description")){
                vector<string>* tmp=parsedMessage(s,':');
                description=(*tmp).at(1);
                delete tmp;

            }*/
        if (startsWith(s,"destination"))
        {
            vector<string> tmp = splitString(s, ":");
            channel = (tmp).at(1);
            
        }
    }
    if(UserChannelEventMap.count(channel + "," + user)==0){
        insertToUserChannelMap(channel,user);
    }
    UserChannelEventMapLock.lock();
    (UserChannelEventMap).at(channel + "," + user).push_back(*(new Event(message)));
    UserChannelEventMapLock.unlock();
    
}
bool StompProtocol::startsWith(const std::string& str, const std::string& prefix) {
    if (prefix.size()>str.size()) return false;
    return str.substr(0,prefix.size()) == prefix;
}
// int StompProtocol::getFromIdMap(int key)
// {
//     return ReceiptIdToSubscriptionId.at(key);
// }
std::vector<std::string> StompProtocol::splitString(const std::string &str, string delimiter)//fixed
{
    std::vector<std::string> tokens;
    boost::split(tokens, str, boost::is_any_of(delimiter));
    return tokens;
}
bool StompProtocol::contains(const vector<string> &parsedMsg,const string& target)
{
    for (string s : parsedMsg)
    {
        if (s == target)
            return true;
    }
    return false;
}
string StompProtocol::createLoginFrame(const string& s)//fixed
{
    vector<string> vec = splitString(s, " ");
    vector<string> hostPort = splitString((vec).at(1), ":");
    std::string answer("CONNECT\naccept-version:1.2\nhost:stomp.cs.bgu.ac.il\nlogin:" + vec.at(2) + "\npasscode:" + vec.at(3));
    
    return answer;
}
string StompProtocol::createJoinFrame(const string& s)//fixed
{
    vector<string> vec = splitString(s, " ");
    std::string answer(std::string("SUBSCRIBE\n") + "destination:" + vec.at(1) + "\nid:" + (std::to_string(ClientCounter)) + "\nreceipt:" + (std::to_string(ReceiptCounter)));
    // insertToMap((*ReceiptCounter),"SUBSCRIBE "+(*vec).at(1)); is unnecessary?
    insertToChannelMap(ClientCounter, vec.at(1));
    insertToFrameMap(ReceiptCounter, "SUBSCRIBE");
    insertToReceiptIdFromSubscriptionId((ReceiptCounter), (ClientCounter));
    (ReceiptCounter)++;
    (ClientCounter)++;
    return answer;
}
string StompProtocol::createExitFrame(const string& s)//fixed
{
    vector<string> vec = splitString(s, " ");
    int subscriberID = getIDFromChannelMap(vec.at(1));
    if(subscriberID==-1){
        return "";
    }
    std::string answer(std::string("UNSUBSCRIBE\nid:") + (std::to_string(subscriberID)) + "\nreceipt:" + (std::to_string(ReceiptCounter))+"\n\n");
    insertToChannelMap(subscriberID, vec.at(1));
    insertToFrameMap(ReceiptCounter, "UNSUBSCRIBE");
    insertToReceiptIdFromSubscriptionId((ReceiptCounter), (subscriberID));
    (ReceiptCounter)++;
    return answer;
}
vector<string> StompProtocol::createReportFrame(const std::string &filePath)//fixed
{
    names_and_events parsedData = parseEventsFile(filePath);
    vector<string> answer{};
    for (const Event &e : parsedData.events)
    {
        string newSend = "";
        newSend.append("SEND\n");
        newSend.append("destination: " + parsedData.channel_name + "\n");//added " "
        newSend.append("\nuser: " + username + "\n");
        newSend.append("city: " + e.get_city() + "\n");
        newSend.append("event name: " + e.get_name() + "\n");
        newSend.append("date time: " + std::to_string(e.get_date_time()) + "\n");
        newSend.append("general information:\n");
        std::map<string, string> generalInformation = e.get_general_information();
        for (pair<string, string> p : generalInformation)
        {
            newSend += "\t" + p.first + ":" + p.second + "\n";
        }
        newSend.append("description:\n");
        newSend.append(e.get_description());
        answer.push_back(newSend);
    }
    return answer;
}
void StompProtocol::createSummaryFrame(const string& channel,const string& username,const string& filePath)//fixed
{
    std::ofstream outFile(filePath);
    outFile << summarize(channel+","+username);
    outFile.close();
}
string StompProtocol::summarize(const string& hashKey)//fixed
{
    if(UserChannelEventMap.count(hashKey)==0){
        std::cout << "No such channel+user combination exists" << std::endl;
        return "Empty summary: request for non-existing channel";
    }
    else if(UserChannelEventMap.at(hashKey).size()==0){
        std::cout << "No reports recieved for this channel" << std::endl;
        return "Empty summary: No reports recieved for this channel";
    }
    else {
        int reportNum = UserChannelEventMap.at(hashKey).size(); // returns size of vector with events
        int activeCounter = 0;
        int arriveAtSceneCounter = 0;
        int reportCounter = 0;
        string bodyanswer;
        string answer;
        std::vector<Event> events = UserChannelEventMap.at(hashKey);
        std::sort(events.begin(), events.end(), [](const Event& a, const Event& b) {
            if (a.get_date_time() != b.get_date_time()) {
                return a.get_date_time() < b.get_date_time(); //Sort by epoch time
            }
            return a.get_name() < b.get_name(); //Sort lexicographically by event name
        });
        for (const Event& e : events)
        {
            string body="";
            reportCounter++;
            body += "Report_" + std::to_string(reportCounter) + ":\n";
            for (const auto &pair : e.get_general_information())
            {
                if (pair.first == "active" && pair.second == "true")
                {
                    activeCounter++;
                }
                if (pair.first == "forces_arrival_at_scene" && pair.second == "true")
                {
                    arriveAtSceneCounter++;
                }
            }
            body += "\tcity:" + e.get_city() + "\n";
            body += "\tdate time: " + epoch_to_date(e.get_date_time()) + "\n";
            body += "\tevent name:" + e.get_name() + "\n";
            if (e.get_description().length() > 27)
            {
                body += "\tsummary: " + e.get_description().substr(0, 27) + "...\n";
            }
            else
            {
                body += "\tsummary: " + e.get_description() + "\n\n";
            }
            bodyanswer+=body;
        }
        vector<string> channel = splitString(hashKey,",");
        string topAnswer("Channel " + channel[0] + "\nStats:\nTotal: " +
                                    std::to_string(reportNum) + "\nactive: " +
                                    std::to_string(activeCounter) + "\nforces arrival at scene: " +
                                    std::to_string(arriveAtSceneCounter) + "\n\nEvent Reports:\n\n");
        answer = (topAnswer) + bodyanswer;
        return answer;
    }
}
string StompProtocol::createLogoutFrame(const string& s)
{
    string out = "DISCONNECT";
    int disconnectReceipt = ReceiptCounter;
    out = out + '\n' + "receipt:" + to_string(disconnectReceipt) + "\n\n";
    insertToFrameMap(ReceiptCounter, "DISCONNECT");
    (ReceiptCounter)++;
    return out;
}
void StompProtocol::insertToFrameMap(int key, const string &frame)
{
    idToFrameMapLock.lock();
    (idToFrameMap)[key] = frame;
    idToFrameMapLock.unlock();
}
const string &StompProtocol::getFromFrameMap(int key)
{
    std::lock_guard<std::mutex> lock(idToFrameMapLock);
    const string &answer = (idToFrameMap)[key];
    return answer;
}
void StompProtocol::insertToChannelMap(int key, const string &channelName)
{
    SubscriptionIdToChannelLock.lock();
    (SubscriptionIdToChannel)[key] = channelName;
    SubscriptionIdToChannelLock.unlock();
}
const string &StompProtocol::getFromChannelMap(int key)
{
    std::lock_guard<std::mutex> lock(SubscriptionIdToChannelLock);
    const string &s = (SubscriptionIdToChannel)[key];
    return s;   
}
const int &StompProtocol::getIDFromChannelMap(const std::string &channelname){
    static const int invalid = -1;
    std::lock_guard<std::mutex> lock(SubscriptionIdToChannelLock);
    for (const auto& it : SubscriptionIdToChannel) {
        if (it.second == channelname) {
            return it.first;
        }
    }
    std::cout << "Cannot exit a channel you're not subscribed to" << std::endl;
    return invalid;
}

void StompProtocol::insertToReceiptIdFromSubscriptionId(int key, int subscribtionID)
{
    ReceiptIdToSubscriptionIdLock.lock();
    (ReceiptIdToSubscriptionId)[key] = subscribtionID;
    ReceiptIdToSubscriptionIdLock.unlock();
}
void StompProtocol::insertToUserChannelMap(std::string channel, std::string user)
{
    vector<Event> empty;
    UserChannelEventMapLock.lock();
    (UserChannelEventMap)[channel+","+user] = empty;
    UserChannelEventMapLock.unlock();
}
const int &StompProtocol::getFromReceiptIdSubscriptionId(int key)
{
    std::lock_guard<std::mutex> lock(ReceiptIdToSubscriptionIdLock);
    const int &s=(ReceiptIdToSubscriptionId)[key];
    return s;
}
// void insertToMap(int key,const string& channelName){//???

// }
vector<string> StompProtocol::firstLogin()
{
    while (1)
    {
        const short bufsize = 1024;
        char buf[bufsize];
        std::cin.getline(buf, bufsize);
        std::string line(buf);
        vector<string> parsedCommand;
        boost::split(parsedCommand, line, boost::is_any_of(" "));
        if (parsedCommand[0] != "login")
        {
            std::cout << "First of all, you must attempt to login..." << std::endl;
        }
        else
        {
            return parsedCommand;
        }
    }
}
bool StompProtocol::checkLoginResponse(ConnectionHandler &ch)
{
    std::string answer;
    if (!(ch).getLine(answer))
    {
        std::cout << "Disconnected. Exiting...\n"
                  << std::endl;
        return false;
    }
    int len = answer.length();
    answer.resize(len - 1);
    vector<string> vec = splitString(answer, "\n");
    if ((vec).at(0) == "CONNECTED")
    {
        cout << "login successful" << std::endl;
        return true;
    }
    if((vec).at(0) == "ERROR")
    {
        for(string s : vec){
            std::cout << s << std::endl;
        }
    }
    return false;
}
std::string StompProtocol::epoch_to_date(time_t e){
    std::tm* tm = std::localtime(&e);

    // Format the time into a string
    char buffer[20];
    std::strftime(buffer, sizeof(buffer), "%d/%m/%Y_%H:%M", tm);

    return std::string(buffer);
}
