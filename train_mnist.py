import mxnet as mx
import argparse
import logging

# load data
def get_mnist_iter(args):
    train_image = args.data_url + 'train-images-idx3-ubyte'
    train_lable = args.data_url + 'train-labels-idx1-ubyte'
    train = mx.io.MNISTIter(image=train_image,
                            label=train_lable,
                            data_shape=(1, 28, 28),
                            batch_size=128,
                            shuffle=True,
                            flat=False,
                            silent=False,
                            seed=10)
    return train

# create network
def get_symbol(num_classes=10, **kwargs):
    data = mx.symbol.Variable('data')
    data = mx.sym.Flatten(data=data)
    fc1  = mx.symbol.FullyConnected(data = data, name='fc1', num_hidden=128)
    act1 = mx.symbol.Activation(data = fc1, name='relu1', act_type="relu")
    fc2  = mx.symbol.FullyConnected(data = act1, name = 'fc2', num_hidden = 64)
    act2 = mx.symbol.Activation(data = fc2, name='relu2', act_type="relu")
    fc3  = mx.symbol.FullyConnected(data = act2, name='fc3', num_hidden=num_classes)
    mlp  = mx.symbol.SoftmaxOutput(data = fc3, name = 'softmax')
    return mlp

def fit(args):
    # create kvstore
    kv = mx.kvstore.create(args.kv_store)
    # logging
    head = '%(asctime)-15s Node[' + str(kv.rank) + '] %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=head)
    logging.info('start with arguments %s', args)
    # get train data
    train = get_mnist_iter(args)
    # create checkpoint
    checkpoint = mx.callback.do_checkpoint(args.model_prefix if kv.rank == 0 else "%s-%d" % (
        args.model_prefix, kv.rank))
    # create callbacks after end of every batch
    batch_end_callbacks = [mx.callback.Speedometer(args.batch_size, args.disp_batches)]
    # get the created network 
    network = get_symbol(num_classes=args.num_classes)
    # create context
    devs = mx.cpu() if args.num_gpus == 0 else [mx.gpu(int(i)) for i in range(args.num_gpus)]
    # create model
    model = mx.mod.Module(context=devs, symbol=network)
    # create an initialization method
    initializer = mx.init.Xavier(rnd_type='gaussian', factor_type="in", magnitude=2)
    # create params of optimizer
    optimizer_params = {'learning_rate': args.lr, 'wd' : 0.0001}
    # run
    model.fit(train,
              begin_epoch=0,
              num_epoch=args.num_epochs,
              eval_data=None,
              eval_metric=['accuracy'],
              kvstore=kv,
              optimizer='sgd',
              optimizer_params=optimizer_params,
              initializer=initializer,
              arg_params=None,
              aux_params=None,
              batch_end_callback=batch_end_callbacks,
              epoch_end_callback=checkpoint,
              allow_missing=True)

if __name__ == '__main__':
    # parse args
    parser = argparse.ArgumentParser(description="train mnist",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--num_classes', type=int, default=10,
                        help='the number of classes')
    parser.add_argument('--num_examples', type=int, default=60000,
                        help='the number of training examples')

    parser.add_argument('--data_url', type=str, default='s3://myobs-for-dls/data/mnist/', help='the training data')
    parser.add_argument('--lr', type=float, default=0.05,
                        help='initial learning rate')
    parser.add_argument('--num_epochs', type=int, default=20,
                        help='max num of epochs')
    parser.add_argument('--disp_batches', type=int, default=20,
                        help='show progress for every n batches')
    parser.add_argument('--batch_size', type=int, default=32,
                        help='the batch size')
    parser.add_argument('--kv_store', type=str, default='device',
                        help='key-value store type')
    parser.add_argument('--model_prefix', type=str, default='s3://myobs-for-dls/working/',
                        help='model prefix')
    parser.add_argument('--num_gpus', type=int, default='0',
                        help='number of gpus')
    args, unkown = parser.parse_known_args()

    fit(args)
