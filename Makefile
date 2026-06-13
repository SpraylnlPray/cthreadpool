all:
	cd lib && make all
	cd bin && make all

clean:
	cd lib && make clean
	cd bin && make clean