.PHONY: all test clean

all: test
	gradle installDist
	rsync -avz --link-dest=../build/install/bkup build/install/bkup/ dist/

test:
	gradle test

clean:
	gradle clean
