CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -g
LIBS     = -lpthread

all: node client

node: nodes.cpp peers.h resp.h
	$(CXX) $(CXXFLAGS) -o node nodes.cpp $(LIBS)

client: client.cpp
	$(CXX) $(CXXFLAGS) -o client client.cpp

clean:
	rm -f node client

.PHONY: all clean