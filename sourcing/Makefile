CC=g++
CFLAGS=-Wall -Werror --std=c++17
CLIBS= -larrow -lparquet

sourcing: sourcing_script.cpp
	$(CC) -o sourcing sourcing_script.cpp $(CFLAGS) $(CLIBS)

.PHONY: clean

clean:
	rm sourcing *.o