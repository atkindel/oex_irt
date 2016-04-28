#!/usr/bin/env bash
echo "Close ssh tunnel..."
ssh -S $DS_USER -O exit $DS_USER@$DS_HOST
