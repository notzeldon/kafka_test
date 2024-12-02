#!/bin/sh
python3 main.py init --topic hello_topic --kafka kafka:9093
python3 main.py consume --topic hello_topic --kafka kafka:9093
