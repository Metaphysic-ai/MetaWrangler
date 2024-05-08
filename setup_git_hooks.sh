#!/bin/bash
cp -p hooks/* .git/hooks/
chmod +x .git/hooks/*
echo "Hooks copied to .git/hooks/"