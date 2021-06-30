SUBDIRS = pegasus-keg #pegasus-mpi-keg

subdirs:
	for dir in $(SUBDIRS); do \
    	$(MAKE) -C $$dir; \
		mv $$dir/$$dir .. ; \
	done

clean:
	for dir in $(SUBDIRS); do \
    	$(MAKE) -C $$dir clean; \
	done
