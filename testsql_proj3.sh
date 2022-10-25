#! /bin/bash
count=100

i=1
while(($i <= $count))
do
	echo "round `expr $i`"
	make test-proj3 >> log/proj3/$i.log
	let "i++"
done
