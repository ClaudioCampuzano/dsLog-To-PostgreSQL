# dsLog-To-PostgreSQL

```
sudo apt install virtualenv
virtualenv env --python=python3
source env/bin/activate
pip3 install -r requirements.txt
python3 main.py -n 0 -f ds_flujo.log -a ds_aforo.log
```

```
chmod +x exec.sh 
./exec.sh 
```

*/10 * * * * bash /home/omia/dsLog-To-PostgreSQL/exec.sh
