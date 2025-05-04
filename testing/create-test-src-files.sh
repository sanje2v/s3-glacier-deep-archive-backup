#!/usr/bin/env bash

mkdir -p /tmp/thisdir
head -c 49M /dev/urandom > /tmp/thisdir/test1.dat
head -c 61M /dev/urandom > /tmp/thisdir/test2.dat
head -c 42M /dev/urandom > /tmp/thisdir/test3.dat
head -c 101M /dev/urandom > /tmp/thisdir/test4.dat
head -c 201M /dev/urandom > /tmp/thisdir/test5.dat
head -c 301M /dev/urandom > /tmp/thisdir/test6.dat
head -c 51M /dev/urandom > /tmp/thisdir/test7.dat
head -c 1M /dev/urandom > /tmp/thisdir/test8.dat
head -c 21M /dev/urandom > /tmp/thisdir/test9.dat
