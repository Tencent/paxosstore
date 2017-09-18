set +ex

base_dir=$(cd `dirname $0`;pwd)
$base_dir/install_protobuf.sh
$base_dir/install_snappy.sh
$base_dir/install_leveldb.sh
