SRC = src

.PHONY: all src

all: src

src: clean
	$(MAKE) -C $(SRC)
	mkdir -p bin
	mv pegasus-keg bin
	mv pegasus-mpi-keg bin/decaf/

clean:
	$(MAKE) -C $(SRC) clean
