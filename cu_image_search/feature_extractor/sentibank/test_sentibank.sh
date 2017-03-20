#!/bin/sh
echo 'Testing CPU extraction'
./extract_nfeatures_gpu caffe_sentibank_train_iter_250000 test.prototxt fc7 test_fc7_cpu 1 'CPU'
echo 'Testing GPU extraction'
./extract_nfeatures_gpu caffe_sentibank_train_iter_250000 test.prototxt fc7 test_fc7_gpu 1 'GPU'
