
ssh spark1 "python /usr/local/hadoop/convertsas.py objectstore" &
sleep 10

ssh spark2 "python /usr/local/hadoop/convertsas.py objectstore" &
sleep 10

ssh spark3 "python /usr/local/hadoop/convertsas.py objectstore"
