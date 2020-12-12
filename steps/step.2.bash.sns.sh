#!/bin/bash

# Send message to SNS Topic
  aws sns publish \
    --topic-arn "$1" \
    --subject "$2" \
    --message "$3" 
    
# IS_MASTER=false;if grep isMaster /mnt/var/lib/info/instance.json | grep true; then
#   IS_MASTER=true
# fi

# if [ "$IS_MASTER" = false ]; then
  
# fi
