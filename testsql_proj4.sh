#! /bin/bash
count=100

i=1
while(($i <= $count))
do
	echo "round `expr $i`"
	make test-proj4-1 >> log/proj4-1/$i.log
	let "i++"
done

j=1
while(($j <= $count))
do
	echo "round `expr $j`"
	make test-proj4-1-extra >> log/proj4-1/extra-$j.log
	let "j++"
done
