c=1
virtual_ip=10.0.0.100
port1=8080
port2=9090
while true
do
if [ $(( $RANDOM % 2 )) -eq 0 ]
then
 curl $virtual_ip:$port1
else
 curl $virtual_ip:$port2
fi 
sleep 0.2
done
