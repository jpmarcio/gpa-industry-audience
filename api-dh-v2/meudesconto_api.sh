#!/bin/bash
ROOT=/home/ec2-user/api-dh
ROOTV2=/home/ec2-user/api-dh-v2
PID=`pgrep uwsgi`

case "$2" in
prod)
    echo $2
    ;;
dev)
    echo $2
    ;;
*)
    echo "Usage: $0 {start|stop|status|restart} {prod/dev}"
    exit 0
    ;;
esac

case "$1" in
start)
  
   if [ -z "$PID" ]; then
      cd $ROOT
	  echo "starting $2 server at 9999"
      nohup /usr/local/bin/uwsgi --http :9999 --wsgi-file /home/ec2-user/api-dh/wsgi_$2.py --master --processes 4 --threads 2 --stats 127.0.0.1:9191 --http-timeout 600  &
	  cd $ROOTV2
	  echo "starting $2 server at 9998"
	  nohup /usr/local/bin/uwsgi --http :9998 --wsgi-file /home/ec2-user/api-dh-v2/wsgi_$2.py --master --processes 4 --threads 2 --stats 127.0.0.1:9192 --http-timeout 600  &
   else
      echo "server is already running, pid=$PID"
      exit 1
   fi
   ;;

stop)
   echo "stoping gunicorn server"
   pkill -9 uwsgi
   ;;

restart)
   pkill -9 uwsgi
   echo "restarting $2 server at 9999"

   cd $ROOT
   nohup /usr/local/bin/uwsgi --http :9999 --wsgi-file /home/ec2-user/api-dh/wsgi_$2.py --master --processes 4 --threads 2 --stats 127.0.0.1:9191 --http-timeout 600  &
   echo "starting $2 server at 9998"
   cd $ROOTV2
   nohup /usr/local/bin/uwsgi --http :9998 --wsgi-file /home/ec2-user/api-dh-v2/wsgi_$2.py --master --processes 4 --threads 2 --stats 127.0.0.1:9192 --http-timeout 600  &
   ;;

status)
   if [ -z "$PID" ];then
      echo $0 is NOT running
   else
         echo $0 is running, pid=$PID
   fi
   exit 0
   ;;
*)
   echo "Usage: $0 {start|stop|status|restart} {prod/dev}"
esac

exit 0

