#/bin/bash

rundate=$1
projectpath=$(cat ~/PROJECT_PATH.txt)
pyfile="/DS_MARKET/MarketInfo/Swap_Update.py"
pypath=${projectpath}${pyfile}
python ${pypath} $rundate
rs=$?
if [ $rs -eq 0 ]; then
    exit 0
else
   exit 1
fi