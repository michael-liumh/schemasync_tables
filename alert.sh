#!/usr/bin/env bash

user=reader
pass=123456

targets=(
    mysql://$user:$pass@172.16.0.30:3306/biz_105
)

for t in ${targets[@]}
do
    /opt/venv4pypy3/bin/pypy /opt/schemasync_tables/schemasync_tables.py --source mysql://$user:$pass@172.16.0.9:3306/biz --target $t --only-sync-exists-tables --sync-comments --url https://open.feishu.cn/open-apis/bot/v2/hook/d844a9ea-85d1-4b1a-8f80-841f17f2edf4;
done
