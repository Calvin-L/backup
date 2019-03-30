.PHONY: all test clean

all: test
	gradle distTar
	$(RM) -r dist
	tar xf build/distributions/glacier-backup-1.0.tar
	mv glacier-backup-1.0 dist

test:
	gradle test

clean:
	gradle clean
