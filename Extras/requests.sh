#valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file="val1.txt" ./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > a.txt &
#valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file="val2.txt" ./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > b.txt


#./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query2.txt &
#./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query2.txt 

./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > a.txt &
./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > b.txt