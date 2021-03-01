CC=g++
CFLAGS=-Wall -Werror --std=c++17
INC_DIF=../thirdparty/arrow/cpp/release/src
LIB_DIR=../thirdparty/arrow/cpp/release/release

sourcing: sourcing_script.cpp
	$(CC) -o sourcing sourcing_script.cpp $(CFLAGS) -L$(LIB_DIR) -larrow

.PHONY: clean

clean:
	rm sourcing *.o