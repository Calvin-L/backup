
all: lib/aescrypt-3.2.jar
	gradle distTar
	$(RM) -r dist
	tar xf build/distributions/glacier-backup-1.0.tar
	mv glacier-backup-1.0 dist

lib:
	mkdir -p '$@'

aescrypt-java-3_2.zip:
	curl -OLf 'https://www.aescrypt.com/download/v3/java/aescrypt-java-3_2.zip'

aescrypt-java-3.2/bin/es/vocali/util/AESCrypt.class: | aescrypt-java-3_2.zip
	unzip '$<'

lib/aescrypt-3.2.jar: aescrypt-java-3.2/bin/es/vocali/util/AESCrypt.class | lib
	jar cvf '$@' -C aescrypt-java-3.2/bin es/vocali/util/AESCrypt.class
