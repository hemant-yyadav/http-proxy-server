CC = gcc
CFLAGS = -g -Wall -Wextra -Wpedantic
LDLIBS = -lpthread

all: proxy

proxy: proxy_parse.o proxy.o
	$(CC) $(CFLAGS) -o proxy proxy_parse.o proxy.o $(LDLIBS)

proxy_parse.o: proxy_parse.c proxy_parse.h
	$(CC) $(CFLAGS) -c proxy_parse.c

proxy.o: proxyserver.c proxy_parse.h
	$(CC) $(CFLAGS) -c proxyserver.c

clean:
	rm -f proxy *.o

tar:
	tar -cvzf ass1.tgz proxyserver.c README.md Makefile proxy_parse.c proxy_parse.h
