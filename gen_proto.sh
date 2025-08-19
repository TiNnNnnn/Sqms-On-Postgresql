#protoc --cpp_out=. ./src/include/storage/storage.proto
protoc --c_out=. ./src/include/collect/format.proto 
#protoc --proto_path=./src/include/collect --c_out=./src/include/collect ./src/include/collect/eq.proto ./src/include/collect/format.proto

