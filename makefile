.PHONY: proto clean

proto:
	protoc -I=proto --go_out=proto proto/*.proto

clean:
	$(RM) proto/*.pb.go