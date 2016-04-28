#!/usr/bin/env bash
echo "Open ssh tunnel..."
ssh -M -S $DS_USER -L 3306:127.0.0.1:3306 $DS_USER@$DS_HOST -fN
