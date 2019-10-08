#!/bin/bash

cd pgxc_ctl 
chmod +x make_signature 
./make_signature 
cd ../

for d in `ls`
do
   if [ -d $d ];
   then
       echo $d
       cd $d
       make clean ; make -sj ; make install
       cd ../ && ls
   fi;
done;
