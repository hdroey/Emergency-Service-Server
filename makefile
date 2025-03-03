CFLAGS:=-c -Wall -Weffc++ -g -std=c++11 -Iinclude
LDFLAGS:=-lboost_system -lpthread
# TODO change the makefile for StompEMIlient
all: clean compile link run

EchoClient: bin/ConnectionHandler.o bin/echoClient.o
	g++ -o bin/EchoClient bin/ConnectionHandler.o bin/echoClient.o $(LDFLAGS)

StompEMIClient: bin/ConnectionHandler.o bin/StompClient.o bin/event.o
	g++ -o bin/StompEMIClient bin/ConnectionHandler.o bin/StompClient.o bin/event.o $(LDFLAGS)

bin/ConnectionHandler.o: src/ConnectionHandler.cpp
	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

bin/echoClient.o: src/echoClient.cpp
	g++ $(CFLAGS) -o bin/echoClient.o src/echoClient.cpp

bin/event.o: src/event.cpp
	g++ $(CFLAGS) -o bin/event.o src/event.cpp

bin/StompClient.o: src/StompClient.cpp
	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

.PHONY: clean
clean:
	rm -f bin/*
compile:
	g++ -g -Wall -Weffc++ -std=c++11 -c -Iinclude src/*.cpp -pthread
	mv *.o bin/ 

link:
	g++ -g -Wall -Weffc++ -std=c++11 -o bin/StompEMIClient bin/*.o -pthread

run:
	bin/StompEMIClient
	
# CFLAGS:=-c -Wall -Weffc++ -g -std=c++11 -Iinclude
# LDFLAGS:=-lboost_system -lpthread

# all: StompEMIClient

# EchoClient: bin/ConnectionHandler.o bin/echoClient.o
# 	g++ -o bin/EchoClient bin/ConnectionHandler.o bin/echoClient.o $(LDFLAGS)

# StompEMIClient: bin/ConnectionHandler.o bin/StompClient.o bin/event.o
# 	g++ -o bin/StompEMIClient bin/ConnectionHandler.o bin/StompClient.o bin/event.o $(LDFLAGS)

# bin/ConnectionHandler.o: src/ConnectionHandler.cpp
# 	g++ $(CFLAGS) -o bin/ConnectionHandler.o src/ConnectionHandler.cpp

# bin/echoClient.o: src/echoClient.cpp
# 	g++ $(CFLAGS) -o bin/echoClient.o src/echoClient.cpp

# bin/event.o: src/event.cpp
# 	g++ $(CFLAGS) -o bin/event.o src/event.cpp

# bin/StompClient.o: src/StompClient.cpp
# 	g++ $(CFLAGS) -o bin/StompClient.o src/StompClient.cpp

# .PHONY: clean
# clean:
# 	rm -f bin/*
	