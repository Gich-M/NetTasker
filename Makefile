CC = gcc
CFLAGS = -Wall -Werror -Wextra -pedantic -std=gnu99 -fstack-protector -fsanitize=address
PYTHON_INCLUDE = $(shell python3 -c "import sysconfig; print(sysconfig.get_path('include'))")
PYTHON_LIB = $(shell python3 -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")
LDFLAGS = -L$(PYTHON_LIB) -lpython3.11 -lpthread -ljson-c -lcurl

SRC_DIR = src/workers
SRCS = $(SRC_DIR)/main.c $(SRC_DIR)/network.c $(SRC_DIR)/python_interface.c
TARGET = $(SRC_DIR)/worker

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -I$(PYTHON_INCLUDE) -o $@ $^ $(LDFLAGS)

.PHONY: clean
clean:
	rm -f $(TARGET)

.PHONY: run
run: $(TARGET)
	./run_system.sh