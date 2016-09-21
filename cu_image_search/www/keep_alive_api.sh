while True;
do
    python api.py &> logAPI$(date +%Y-%m-%d).txt;
    echo "["$(date)"] API crashed." >> logAPI_keep_alive.txt;
    sleep 5;
done
