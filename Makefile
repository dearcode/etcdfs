all: etcdfs

.PHONY: etcdfs

clean:
	@rm -rf bin

etcdfs:
	go build -o bin/$@ cmd/$@/main.go 
				     		                       			


