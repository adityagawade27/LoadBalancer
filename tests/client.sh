c=1
while true
do
 fortune | nc -l $1 > /dev/null
 echo "Number of packets: $c"
 (( c++ ))
done
