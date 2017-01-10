
all:
	gradle distTar
	$(RM) -r dist
	tar xf build/distributions/glacier-backup-1.0.tar
	mv glacier-backup-1.0 dist
