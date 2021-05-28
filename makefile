
#
# 
#	Muhammed Okumus - 151044017
#

OBJS	= server.o client.o
OUT	= server client

OBJS0	= server.o
SOURCE0	= server.c
HEADER0	= 
OUT0	= server

OBJS1	= client.o
SOURCE1	= client.c
HEADER1	= 
OUT1	= client

CC	 = gcc
FLAGS	 = -g -c -Wall
LFLAGS	 = -lpthread -lm

all: server client

server: $(OBJS0) 
	$(CC) -g $(OBJS0) -o $(OUT0) $(LFLAGS)

client: $(OBJS1)
	$(CC) -g $(OBJS1) -o $(OUT1)

server.o: server.c
	$(CC) $(FLAGS) server.c 

client.o: client.c
	$(CC) $(FLAGS) client.c 


clean:
	rm -f $(OBJS) $(OUT)