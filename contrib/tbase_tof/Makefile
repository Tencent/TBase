CFLAGS += -Werror -Wall
all: tbase_tof.so

clean:
	$(RM) tbase_tof.so *.o

tbase_tof.so: src/tbase_tof.c
	$(CC) $(CFLAGS) -export-dynamic -shared -export-dynamic -I./include -lcjson -lpam -L./lib -fPIC -shared -Xlinker -x -o $@ $< -lcurl -g 
