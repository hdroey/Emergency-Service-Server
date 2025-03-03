#pragma once
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <mutex>
#include <unordered_map>
#include "ConnectionHandler.h"
#include "event.h"
#include <unordered_map>
#include "boost/algorithm/string.hpp"
using namespace std;
#include "../include/ConnectionHandler.h"

// TODO: implement the STOMP protocol
extern int ReceiptCounter;
extern int ClientCounter;
extern std::string username;
extern ConnectionHandler *ch;
extern bool isConnected;
extern std::mutex SubscriptionIdToChannelLock;
extern std::mutex idToFrameMapLock;
extern std::mutex ReceiptIdToSubscriptionIdLock;
extern std::mutex UserChannelEventMapLock;
extern std::unordered_map<int, std::string> SubscriptionIdToChannel;
extern std::unordered_map<int, std::string> idToFrameMap;
extern std::unordered_map<int, int> ReceiptIdToSubscriptionId;
extern std::unordered_map<std::string, std::vector<Event>> UserChannelEventMap;
class StompProtocol
{
public:
    int main(int argc, char *argv[]);
    void readConsoleTask(ConnectionHandler &ch);
    void readSocketTask(ConnectionHandler &ch);
    void handleMessageFrame(const std::string &message);
    bool startsWith(const std::string &str, const std::string &prefix);
    int getFromIdMap(int key);
    std::vector<std::string> splitString(const std::string &str, string delimiter);
    bool contains(const std::vector<std::string> &parsedMsg, const std::string &target);
    std::string createLoginFrame(const std::string &s);
    std::string createJoinFrame(const std::string &s);
    std::string createExitFrame(const std::string &s);
    std::vector<std::string> createReportFrame(const std::string &filePath);
    void createSummaryFrame(const std::string &channel, const std::string &username, const std::string &filePath);
    string summarize(const std::string &hashKey);
    std::string createLogoutFrame(const std::string &s);
    void insertToFrameMap(int key, const std::string &frame);
    const std::string &getFromFrameMap(int key);
    void insertToChannelMap(int key, const std::string &channelName);
    const std::string &getFromChannelMap(int key);
    const int &getIDFromChannelMap(const std::string &channelname);
    void insertToReceiptIdFromSubscriptionId(int key, int subscriptionID);
    void insertToUserChannelMap(const std::string channel, const std::string username);
    const int &getFromReceiptIdSubscriptionId(int key);
    std::vector<std::string> firstLogin();
    bool checkLoginResponse(ConnectionHandler &ch);
    std::string epoch_to_date(time_t e);
};
