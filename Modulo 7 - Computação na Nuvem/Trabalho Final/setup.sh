#!/bin/bash 

sudo apt-get update 
sudo apt-get install apache2 --assume-yes 
sudo chmod 777 /var/www/html 

sudo apt-get install python2.7  --assume-yes 
sudo apt-get install python-pip --assume-yes 
sudo pip install requests
sudo pip install psutil 

wget -c http://alanamaral.com.br/upload/UNI7/FCN/trabalho/primeClientTrabalho.py 
chmod 755 primeClientTrabalho.py

echo "* * * * * run-one /home/ubuntu/primeClientTrabalho.py" | crontab -
