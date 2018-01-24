import numpy as np
import tensorflow as tf

tf.reset_default_graph

# Create inputs for sum(1/n!) for n = [0,1,2,...,12]
n = np.arange(13.000) #create array for n = [0,1,2,...,12]
n = tf.convert_to_tensor(n, dtype = tf.float32) 
a = tf.constant(1.000, name = "a") 

# Create tensor operations for function
fact = tf.exp(tf.lgamma(n + 1)) # Factorial Function (n!)
div = tf.divide(a,fact)         # 1/n!
sumd = tf.reduce_sum(div)       # sum(1/n!) for all n

init = tf.global_variables_initializer()  # initializes all

with tf.Session() as sess:
    sess.run(init)       # run init operation, initialize all
    print("Taylor series approximation for e: sum(1/n!) for n = [0,1,2,...,12]")
    print("\nn! equals: %s" % sess.run(fact))
    print("\n1/n! equals: %s" % sess.run(div))
    print("\nsum(1/n!) n = [0,1,2,...,12] equals: %s" % sess.run(sumd))
    print("\nTherefore, approximation of e = %f" % sess.run(sumd))

    file_writer = tf.summary.FileWriter("output", sess.graph)
    file_writer.add_graph(sess.graph)
    file_writer.flush()
    file_writer.close()
    sess.close()