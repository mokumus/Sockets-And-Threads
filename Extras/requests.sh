#valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file="val1.txt" ./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > a.txt &
#valgrind --leak-check=full --show-leak-kinds=all --track-origins=yes --log-file="val2.txt" ./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query2.txt > b.txt


#./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query2.txt &
#./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query2.txt 

./client -i 1 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c1.txt &
./client -i 2 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c2.txt &
./client -i 3 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c3.txt &
./client -i 4 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c4.txt &
./client -i 5 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c5.txt &
./client -i 6 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c6.txt &
./client -i 7 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c7.txt &
./client -i 8 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c8.txt &
./client -i 9 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c9.txt &
./client -i 10 -a 127.0.0.1 -p 9090 -o Extras/query1.txt > Outputs/c10.txt