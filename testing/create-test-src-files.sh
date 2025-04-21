#!/usr/bin/env bash

mkdir -p /tmp/thisdir
head -c 50M /dev/urandom > /tmp/thisdir/test1.dat
head -c 60M /dev/urandom > /tmp/thisdir/test2.dat
head -c 40M /dev/urandom > /tmp/thisdir/test3.dat
head -c 100M /dev/urandom > /tmp/thisdir/test4.dat
