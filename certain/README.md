# Certain

Certain is an asynchronous implementation of paxos log, but using in synchronous way is supported. And It's widely used in WeChat backend as the base of building distributed system that both high availability and strong consistency are required.

# Build example

The example is a simple card server with full neccesary functions implemetation, such as routing, failover, getall and plog expiring. It's based on gRPC, which allows you to access by clients of different language easily.

    $ sh autobuild.sh example

# Have a try on the example

    Make a directory for the servers first, for example:

    $ mkdir /home/rockzheng/certain

    Run three servers, you could see they listen on diffent port for the client.
    Exactly speaking, 5005x is for card_srv_x, of which x is 0, 1, or 2.

    $ ./card_srv -c example/example.conf -p /home/rockzheng/certain -i 0 &
    $ ./card_srv -c example/example.conf -p /home/rockzheng/certain -i 1 &
    $ ./card_srv -c example/example.conf -p /home/rockzheng/certain -i 2 &

    Use card_tool to do some operations, like Insert/Delete/Update/Select.

    $ ./card_tool
        ./CardTool -X/-Y/-Z addr -o Insert -i <card_id> -n <user_name> -u <user_id> -b <balance>
        ./CardTool -X/-Y/-Z addr -o Update -i <card_id> -d <delta>
        ./CardTool -X/-Y/-Z addr -o Delete -i <card_id>
        ./CardTool -X/-Y/-Z addr -o Select -i <card_id>
        ./CardTool -a       addr -o Recover -i <card_id>

    $ ./card_tool -X 127.0.0.1:50050 -o Insert -i 12358 -n rock -u 20170001 -b 200
        Failure with error: code(8002) msg(card exists)

    $ ./card_tool -X 127.0.0.1:50050 -o Select -i 12358
        user_name=rock user_id=20170001 balance=200
        Done

    $ ./card_tool -X 127.0.0.1:50050 -o Update -i 12358 -d 10
        balance=210
        Done

    $ ./card_tool -X 127.0.0.1:50050 -o Delete -i 12358
        Done

    $ ./card_tool -X 127.0.0.1:50050 -o Select -i 12358
        Failure with error: code(8001) msg(card not exist)

    Trigger GetAll/CatchUp when data is lost.

    $ ./card_tool -X 127.0.0.1:50050 -o Insert -i 12358 -n rock -u 20170001 -b 200
        Failure with error: code(8002) msg(card exists)

    $ rm /home/rockzheng/certain/datadb_0 -rf
    $ rm /home/rockzheng/certain/logdb_0 -rf

    $ ./card_tool -X 127.0.0.1:50050 -o Select -i 12358
        user_name=rock user_id=20170001 balance=200
        Done
