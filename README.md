# dsLog-To-PostgreSQL

```
sudo apt install virtualenv
virtualenv env --python=python3
source env/bin/activate
pip3 install -U psycopg2-binary
python3 main.py -n 0 -f ds.log -a ds.log
```
