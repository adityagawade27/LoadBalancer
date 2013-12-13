c=1
pcount80=0
pcount90=0
virtual_ip=10.0.0.254
port1=8080
port2=9090
while [ $c -le 20 ]
do
if [ $(( $RANDOM % 2 )) -eq 0 ]
then
 curl $virtual_ip:$port1
 (( pcount80++ ))
else
 curl $virtual_ip:$port2
 (( pcount90++ ))
fi 
(( c++ ))
sleep 0.2
done
echo "Packets sent \n"
echo "port 8080: $pcount80"
echo "port 9090: $pcount90"
