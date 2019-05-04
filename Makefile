.PHONY: all test update-submodules clean

all: test
	gradle installDist
	rsync -avzc --delete --link-dest=../build/install/bkup build/install/bkup/ dist/

test: update-submodules
	gradle test

update-submodules:
	git submodule update --init

clean:
	gradle clean
