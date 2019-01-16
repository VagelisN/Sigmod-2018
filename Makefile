
PROG = ./submission/build/release/radixhash
OBJS = handler.o preprocess.o results.o rhjoin.o query.o inter_res.o filter.o relation_list.o relation_map.o scheduler.o stats.o best_tree.o
SRCS = handler.c preprocess.c results.c rhjoin.c query.c inter_res.c filter.c relation_list.c relation_map.c scheduler.c stats.c best_tree.c
HEADER = preprocess.h results.h rhjoin.h structs.h query.h inter_res.h filter.h relation_list.h relation_map.h scheduler.h stats.h best_tree.h

$(PROG): $(OBJS)
	gcc -Ofast $(OBJS) -o $(PROG) -lpthread -lrt -lm


handler.o: handler.c
	gcc -Ofast -c  handler.c

preprocess.o:
	gcc -Ofast -c preprocess.c

results.o:
	gcc -Ofast -c results.c

rhjoin.o:
	gcc -Ofast -c rhjoin.c

query.o:
	gcc -Ofast -c query.c

inter_res.o:
	gcc -Ofast -c inter_res.c

filter.o:
	gcc -Ofast -c filter.c

relation_map.o:
	gcc -Ofast -c relation_map.c

relation_list.o:
	gcc -Ofast -c relation_list.c

scheduler.o:
	gcc -Ofast -c scheduler.c

stats.o:
	gcc -Ofast -c stats.c

best_tree.o:
	gcc -Ofast -c best_tree.c

clean:
	rm $(OBJS) $(PROG)
