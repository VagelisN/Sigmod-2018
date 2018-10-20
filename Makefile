
PROG = radixhash
OBJS = main.o preprocess.o results.o rhjoin.o 
SRCS = main.c preprocess.c results.c rhjoin.c
HEADER = preprocess.h results.h rhjoin.h structs.h

$(PROG): $(OBJS)
	gcc -g $(OBJS) -o $(PROG)


main.o: main.c
	gcc -g -c main.c
	
preprocess.o:
	gcc -g -c preprocess.c
	
results.o:
	gcc -g -c results.c
	
rhjoin.o:
	gcc -g -c rhjoin.c

clean:
	rm $(OBJS) $(PROG)
