myloc=$(realpath "$0" | sed 's|\(.*\)/.*|\1|')
dirLogs="/home/$(whoami)/Documents/logs"
source  $myloc/env/bin/activate
python3  $myloc/main.py -n 0 -f $dirLogs/flujo/ds.log  -a $dirLogs/aforo/ds.log
