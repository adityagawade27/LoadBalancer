c=1
ip=`ifconfig | grep "inet addr:" | head -1| awk '{print $2}'`
ofile=../info/appservers.info
sudo echo "$ip:$1" >> $ofile
while true
do
 fortune | nc -l $1 > /dev/null
 echo "Number of packets: $c"
 (( c++ ))
done
