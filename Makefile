SRC = src

.PHONY: all src

all: src

src: clean
	$(MAKE) -C $(SRC)
	mkdir -p bin
	mv pegasus-keg bin

clean:
	$(MAKE) -C $(SRC) clean
