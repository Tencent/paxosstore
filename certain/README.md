# certain

Certain is an asynchronous implementation of paxos log.

# build simple_srv and simple_cli

	1) download leveldb(https://github.com/google/leveldb/tree/v1.18), move to third/leveldb-1.18 and make,

	2) download protobuf(https://github.com/google/protobuf/tree/v3.1.0), move to third/protobuf-3.1.0 and make,

	3) download libco(https://github.com/Tencent/libco), move to third/libco-master and make,

	4) cd certain; make -j 10 example;

# play

For server:

	Use '-i' and 'e' to override LocalAcceptorID and ExtEndpoint in the configure.
	If AcceptorNum is 3 in certain.conf, each server(process) run as:

	./simple_srv -c example/example.conf -i 0 -e 127.0.0.1:38240

	./simple_srv -c example/example.conf -i 1 -e 127.0.0.1:38241

	./simple_srv -c example/example.conf -i 2 -e 127.0.0.1:38242

For client:

	~>./simple_cli 127.0.0.1 38240

    Get xxx
    cmd: cmd 3 uuid 409927680 E(560266, 1) scmd 1 key xxx val.size 0 val  ret -7000 // Not Found

    Set xxx yyy
    cmd: cmd 3 uuid 409927682 E(560266, 1) scmd 2 key xxx val.size 3 val yyy ret 0 // OK

    Get xxx
    cmd: cmd 3 uuid 409927683 E(560266, 2) scmd 1 key xxx val.size 3 val yyy ret 0 // OK

To have fun, you may kill some of them, and then restart.
