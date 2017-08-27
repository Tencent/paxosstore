#/bin/bash
cd libco;
rm -rf *.o; rm -f cmake_install.cmake; cd ..;

declare -a arr=("core" "msg_svr" "comm" "dbcomm" "memkv" "kv" "example");
for dir_name in "${arr[@]}"
do
  cd $dir_name; rm -rf CMakeFiles; rm -f Makefile; rm -f *.o; rm -f cmake_install.cmake; cd ../;
done
#cd core;
#rm -rf *.o; rm -f cmake_install.cmake; cd ../;
#cd msg_svr;
#rm -rf CMakeFiles; rm -f Makefile; rm -f *.o; rm -f cmake_install.cmake; cd ../;

#cd comm;
#make clean; cd ..;
#cd dbcomm;
#make clean; cd ..;
#cd memkv;
#make clean; cd ..;
#cd kv;
#make clean; cd ..;
#cd example;
#make clean; cd ..;
