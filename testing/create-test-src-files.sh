#!/usr/bin/env bash

mkdir -p /tmp/thisdir
head -c 49M /dev/urandom > /tmp/thisdir/test1.dat
head -c 61M /dev/urandom > /tmp/thisdir/test2.dat
head -c 42M /dev/urandom > /tmp/thisdir/test3.dat
head -c 101M /dev/urandom > /tmp/thisdir/test4.dat
