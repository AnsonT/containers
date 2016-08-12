FROM kaixhin/caffe

RUN ln /dev/null /dev/raw1394
RUN pip install lmdb

RUN cd $CAFFE_ROOT
RUN ./data/mnist/get_mnist.sh
RUN ./examples/mnist/create_mnist.sh


