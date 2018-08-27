#!/usr/bin/env python

# Copyright 2018 elegans.io ltd All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""item similarity encoder/decoder NN"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import fileinput
import glob
import math
import os
import re
import sys
from itertools import islice

import numpy as np
import tensorflow as tf
from six.moves import xrange
from tensorflow.contrib.tensorboard.plugins import projector


def batch_generator(p_cooccurrence_folder, p_batch_size=512, p_epochs=5):
    files = glob.glob(p_cooccurrence_folder + "/part-*")
    old_n_lines = []
    for e in range(p_epochs):
        with fileinput.input(files=files) as f:
            for n_lines in filter(lambda x: x != [], iter(lambda: list(islice(f, batch_size)), [])):
                len_n_lines = len(n_lines)
                if len_n_lines < p_batch_size:
                    d = p_batch_size - len_n_lines
                    new_n_lines = n_lines + old_n_lines[-d:]
                else:
                    new_n_lines = n_lines
                old_n_lines = n_lines
                pairs = [[int(n) for n in l.strip().split(",")] for l in new_n_lines]
                batch = np.ndarray(shape=(p_batch_size,), dtype=np.int32)
                labels = np.ndarray(shape=(p_batch_size, 1), dtype=np.int32)
                for i in range(batch_size):
                    batch[i] = pairs[i][0]
                    labels[i] = pairs[i][1]
                yield (batch, labels)


def encoder():
    graph = tf.Graph()

    # Build a skip-gram model.
    with tf.device('/gpu:0'), graph.as_default():
        # Input data.
        with tf.name_scope('inputs'):
            train_inputs = tf.placeholder(tf.int32, shape=[batch_size])
            train_labels = tf.placeholder(tf.int32, shape=[batch_size, 1])
            valid_dataset = tf.constant(valid_examples, dtype=tf.int32)

        # Ops and variables pinned to the CPU because of missing GPU implementation
        # Look up embeddings for inputs.
        with tf.name_scope('embeddings'):
            embeddings = tf.Variable(
                tf.random_uniform([vocabulary_size, embedding_size], -1.0, 1.0))
            embed = tf.nn.embedding_lookup(embeddings, train_inputs)

        # Construct the variables for the NCE loss
        with tf.name_scope('weights'):
            nce_weights = tf.Variable(
                tf.truncated_normal(
                    [vocabulary_size, embedding_size],
                    stddev=1.0 / math.sqrt(embedding_size)))

        with tf.name_scope('biases'):
            nce_biases = tf.Variable(tf.zeros([vocabulary_size]))

        # Compute the average NCE loss for the batch.
        # tf.nce_loss automatically draws a new sample of the negative labels each
        # time we evaluate the loss.
        # Explanation of the meaning of NCE loss:
        #   http://mccormickml.com/2016/04/19/word2vec-tutorial-the-skip-gram-model/
        with tf.name_scope('loss'):
            loss = tf.reduce_mean(
                tf.nn.nce_loss(
                    weights=nce_weights,
                    biases=nce_biases,
                    labels=train_labels,
                    inputs=embed,
                    num_sampled=num_sampled,
                    remove_accidental_hits=True,
                    num_classes=vocabulary_size))

        # Add the loss value as a scalar to summary.
        tf.summary.scalar('loss', loss)

        # Construct the SGD optimizer using a learning rate of xxx.
        with tf.name_scope('optimizer'):
            optimizer = tf.train.GradientDescentOptimizer(learning_rate).minimize(loss)

        # Compute the cosine similarity between minibatch examples and all embeddings.
        norm = tf.sqrt(tf.reduce_sum(tf.square(embeddings), 1, keepdims=True))
        normalized_embeddings = embeddings / norm
        valid_embeddings = tf.nn.embedding_lookup(normalized_embeddings,
                                                  valid_dataset)
        similarity = tf.matmul(
            valid_embeddings, normalized_embeddings, transpose_b=True)

        # Merge all summaries.
        merged = tf.summary.merge_all()

        # Add variable initializer.
        init = tf.global_variables_initializer()

        # Create a saver.
        saver = tf.train.Saver()

    # Begin training.
    with tf.device('/gpu:0'), tf.Session(graph=graph) as session:
        # Open a writer to write summaries.
        writer = tf.summary.FileWriter(FLAGS.out_folder, session.graph)

        # We must initialize all variables before we use them.
        init.run()
        print('Initialized: steps(' + str(num_steps) + ') epochs(' + str(epochs) + ')')

        average_loss = 0
        for step in xrange(num_steps):
            batch_inputs, batch_labels = next(batches)
            feed_dict = {train_inputs: batch_inputs, train_labels: batch_labels}

            # Define metadata variable.
            run_metadata = tf.RunMetadata()

            # We perform one update step by evaluating the optimizer op (including it
            # in the list of returned values for session.run()
            # Also, evaluate the merged op to get all summaries from the returned "summary" variable.
            # Feed metadata variable to session for visualizing the graph in TensorBoard.
            _, summary, loss_val = session.run(
                [optimizer, merged, loss],
                feed_dict=feed_dict,
                run_metadata=run_metadata)
            average_loss += loss_val

            # Add returned summaries to writer in each step.
            writer.add_summary(summary, step)
            # Add metadata to visualize the graph for the last run.
            if step == (num_steps - 1):
                writer.add_run_metadata(run_metadata, 'step%d' % step)

            if step % printloss_step == 0:
                if step > 0:
                    average_loss /= printloss_step
                # The average loss is an estimate of the loss over the last 2000 batches.
                print('Average loss at step ', step, ' (' + str(step / num_steps) + '): ', average_loss)
                average_loss = 0

            # Note that this is expensive (~20% slowdown if computed every 500 steps)
            if step % evaluate_step == 0:
                sim = similarity.eval()
                for i in xrange(valid_size):
                    valid_word = valid_examples[i]
                    top_k = 10  # number of nearest neighbors
                    nearest = (-sim[i, :]).argsort()[1:top_k + 1]
                    log_str = 'Nearest to %s:' % valid_word
                    for k in xrange(top_k):
                        close_word = nearest[k]
                        log_str = '%s %s,' % (log_str, close_word)
                    print(log_str)
        final_embeddings = normalized_embeddings.eval()

        # Save the model for checkpoints.
        saver.save(session, os.path.join(FLAGS.out_folder, 'model.ckpt'))

        # Create a configuration for visualizing embeddings with the labels in TensorBoard.
        config = projector.ProjectorConfig()
        embedding_conf = config.embeddings.add()
        embedding_conf.tensor_name = embeddings.name
        embedding_conf.metadata_path = os.path.join(FLAGS.out_folder, 'metadata.tsv')
        projector.visualize_embeddings(writer, config)

        with open(FLAGS.out_folder + '/final_embeddings_' + str(embedding_size) + '.tsv', 'w') as f:
            for i in xrange(vocabulary_size):
                f.write(str(i) + "\t" + ",".join([str(k) for k in final_embeddings[0]]) + '\n')
            writer.close()


current_path = os.path.dirname(os.path.realpath(sys.argv[0]))

parser = argparse.ArgumentParser(description='Train an encoder NN to extract a vector representation from items\n' +
                                 '    create a final_embeddings_<embedding_size>.tsv file inside the output folder')
parser.add_argument('--input-folder', type=str, default=os.path.join(current_path, 'ETL_FOLDER'),
                    help='input data directory with COOCCURRENCE_* folder')
parser.add_argument('--out-folder', type=str, default=os.path.join(current_path, 'ITEM_TO_ITEM_ENCODER'),
                    help='The log directory for TensorBoard summaries.')
FLAGS, unparsed = parser.parse_known_args()

# Create the directory for TensorBoard variables if there is not.
if not os.path.exists(FLAGS.out_folder):
    os.makedirs(FLAGS.out_folder)

# Step 3: Function to generate a training batch for the skip-gram model.
cooccurrence_folders = glob.glob(FLAGS.input_folder + "/COOCCURRENCE_*")
assert(len(cooccurrence_folders) == 1)
cooccurrence_folder = cooccurrence_folders[0]
cooccurrence_basename = os.path.basename(cooccurrence_folder)
cooccurrence_re = re.compile("^COOCCURRENCE_([0-9]+)_([0-9]+)$")
cooccurrence_values = cooccurrence_re.match(cooccurrence_basename).groups()
assert(len(cooccurrence_values) == 2)
max_rank_id = int(cooccurrence_values[0])
cooccurrence_entries = int(cooccurrence_values[1])
vocabulary_size = max_rank_id + 1

batch_size = 512
embedding_size = 16  # Dimension of the embedding vector.
num_sampled = 5  # Number of negative examples to sample.

learning_rate = 0.5

printloss_step = 2000
evaluate_step = 10000

epochs = 5

batches = batch_generator(p_cooccurrence_folder=cooccurrence_folder,
                          p_batch_size=batch_size, p_epochs=epochs)

num_steps = int((cooccurrence_entries * epochs) / batch_size) + 1

# We pick a random validation set to sample nearest neighbors. Here we limit the
# validation samples to the words that have a low numeric ID, which by
# construction are also the most frequent. These 3 variables are used only for
# displaying model accuracy, they don't affect calculation.
valid_size = 10  # Random set of words to evaluate similarity on.
valid_window = 10  # Only pick dev samples in the head of the distribution.
valid_examples = np.random.choice(valid_window, valid_size, replace=False)

encoder()
